use std::{collections::HashMap, sync::Arc};

use kameo::{
    actor::ActorRef,
    message::{Context, Message},
};
use ts_bart::RoutingTable;
use ts_dataplane::overlay_router::{
    inbound::RouteAction as InboundRouteAction, outbound::RouteAction as OutboundRouteAction,
};
use ts_transport::{DynEndpoint, OverlayTransportId, PeerId, UnderlayTransportId};

use crate::{
    Error,
    env::Env,
    multiderp::DerpTransportMap,
    path_discoverer::{BestPathMap, BestPaths},
    peer_tracker::PeerState,
};

pub struct RouteUpdater {
    default_overlay_transport: OverlayTransportId,
    /// Overlay transport for traffic addressed to this node's own tailnet addresses, if it
    /// shouldn't go to `default_overlay_transport`.
    self_overlay_transport: Option<OverlayTransportId>,
    /// Map of all available DERP regions to the ID of the underlay transport that handles that
    /// region.
    derp_transport_map: DerpTransportMap,
    peer_state: Arc<PeerState>,
    best_paths: Arc<BestPathMap>,
    env: Env,
}

impl RouteUpdater {
    fn build_peer_routes(&self) -> PeerRoutesInner {
        tracing::trace!(
            n_peers = self.peer_state.peers.peers().len(),
            "reconstructing routes for peer update"
        );

        let mut routes = PeerRoutesInner::default();

        for (id, peer) in self.peer_state.peers.peers() {
            let span = tracing::trace_span!(
                "peer_route_update",
                peer_key = %peer.node_key,
                region = ?peer.derp_region,
                underlay_transport = tracing::field::Empty,
                ep = tracing::field::Empty,
                peer_id = ?id,
            )
            .entered();

            let (transport_id, ep) = if let Some((transport_id, ep)) = self.best_paths.get(id) {
                (*transport_id, ep.clone())
            } else if let Some(region) = peer.derp_region
                && let Some(transport_id) = self.derp_transport_map.0.get(&region)
            {
                (*transport_id, DynEndpoint::derp(peer.node_key))
            } else {
                // If we would log this error because we don't have a derp map or the peer just
                // doesn't have a home region set in control, skip doing so because it'll be spammy.
                if !self.derp_transport_map.0.is_empty() && peer.derp_region.is_some() {
                    tracing::error!("no region stored in multiderp, no underlay route");
                }

                continue;
            };

            routes
                .underlay_routes
                .insert(*id, (transport_id, ep.clone()));

            span.record("underlay_transport", tracing::field::debug(&transport_id));
            span.record("ep", tracing::field::debug(&ep));

            tracing::trace!(?ep, ?transport_id, "selected route for peer");

            for route in &peer.accepted_routes {
                routes
                    .overlay_out_routes
                    .insert(*route, OutboundRouteAction::Wireguard(*id));
            }
        }

        routes
    }

    async fn publish_peer_routes(&mut self) {
        let new_routes = self.build_peer_routes();

        if let Err(e) = self
            .env
            .publish(PeerRouteUpdate {
                inner: Arc::new(new_routes),
            })
            .await
        {
            tracing::error!(error = %e, "publishing peer route update");
        }
    }
}

impl kameo::Actor for RouteUpdater {
    type Args = (Env, OverlayTransportId, Option<OverlayTransportId>);
    type Error = Error;

    async fn on_start(
        (env, default_transport, self_transport): Self::Args,
        slf: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        env.subscribe::<Arc<PeerState>>(&slf).await?;
        env.subscribe::<Arc<ts_control::StateUpdate>>(&slf).await?;
        env.subscribe::<DerpTransportMap>(&slf).await?;
        env.subscribe::<BestPaths>(&slf).await?;

        env.register(None, &slf).await?;

        Ok(Self {
            default_overlay_transport: default_transport,
            self_overlay_transport: self_transport,
            derp_transport_map: Default::default(),
            peer_state: Default::default(),
            best_paths: Default::default(),
            env,
        })
    }
}

#[derive(Clone)]
pub struct SelfRouteUpdate {
    pub overlay_in_routes: Arc<ts_bart::Table<InboundRouteAction>>,
}

#[derive(Clone)]
pub struct PeerRouteUpdate {
    pub inner: Arc<PeerRoutesInner>,
}

#[derive(Default)]
pub struct PeerRoutesInner {
    pub underlay_routes: HashMap<PeerId, (UnderlayTransportId, DynEndpoint)>,
    pub overlay_out_routes: ts_bart::Table<OutboundRouteAction>,
}

impl Message<Arc<PeerState>> for RouteUpdater {
    type Reply = ();

    async fn handle(&mut self, msg: Arc<PeerState>, _ctx: &mut Context<Self, Self::Reply>) {
        self.peer_state = msg;

        let new_routes = self.build_peer_routes();

        if let Err(e) = self
            .env
            .publish(PeerRouteUpdate {
                inner: Arc::new(new_routes),
            })
            .await
        {
            tracing::error!(error = %e, "publishing peer route update");
        }
    }
}

impl Message<DerpTransportMap> for RouteUpdater {
    type Reply = ();

    async fn handle(&mut self, msg: DerpTransportMap, _ctx: &mut Context<Self, Self::Reply>) {
        if self.derp_transport_map.0 == msg.0 {
            return;
        }

        tracing::debug!("derp transport map changed, building new routes");

        self.derp_transport_map = msg;
        self.publish_peer_routes().await;
    }
}

impl Message<Arc<ts_control::StateUpdate>> for RouteUpdater {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: Arc<ts_control::StateUpdate>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) {
        let Some(node) = msg.node.as_ref() else {
            return;
        };

        tracing::debug!(accepted_routes = ?node.accepted_routes, "populating accepted routes");

        let out = self_routes(
            &node.accepted_routes,
            &node.tailnet_address,
            self.default_overlay_transport,
            self.self_overlay_transport,
        );

        if let Err(e) = self
            .env
            .publish(SelfRouteUpdate {
                overlay_in_routes: Arc::new(out),
            })
            .await
        {
            tracing::error!(error = %e, "publishing self route update");
        }
    }
}

impl Message<BestPaths> for RouteUpdater {
    type Reply = ();

    async fn handle(
        &mut self,
        BestPaths(new_bps): BestPaths,
        _ctx: &mut Context<Self, Self::Reply>,
    ) {
        if self.best_paths == new_bps {
            tracing::trace!("no change to best paths, skip");
            return;
        }

        self.best_paths = new_bps;

        self.publish_peer_routes().await;
    }
}

/// Build the inbound routing table for traffic addressed to this node.
///
/// Accepted routes go to `default_transport` (the netstack), except this node's own tailnet
/// addresses, which go to `self_transport` if there is one.
fn self_routes(
    accepted_routes: &[ipnet::IpNet],
    tailnet_address: &ts_control::TailnetAddress,
    default_transport: OverlayTransportId,
    self_transport: Option<OverlayTransportId>,
) -> ts_bart::Table<InboundRouteAction> {
    let mut out = ts_bart::Table::default();

    for &accepted_route in accepted_routes {
        out.insert(
            accepted_route,
            InboundRouteAction::ToOverlay(default_transport),
        );
    }

    if let Some(self_transport) = self_transport {
        for addr in [
            core::net::IpAddr::from(tailnet_address.ipv4.addr()),
            core::net::IpAddr::from(tailnet_address.ipv6.addr()),
        ] {
            out.insert(addr.into(), InboundRouteAction::ToOverlay(self_transport));
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup(table: &ts_bart::Table<InboundRouteAction>, ip: &str) -> Option<OverlayTransportId> {
        match table.lookup(ip.parse().unwrap()) {
            Some(&InboundRouteAction::ToOverlay(id)) => Some(id),
            _ => None,
        }
    }

    #[test]
    fn self_routes_split() {
        let netstack = OverlayTransportId::from(0);
        let raw = OverlayTransportId::from(1);

        let tailnet_address = ts_control::TailnetAddress {
            ipv4: "100.64.0.1/32".parse().unwrap(),
            ipv6: "fd7a:115c:a1e0::1/128".parse().unwrap(),
        };
        let accepted: Vec<ipnet::IpNet> =
            ["100.64.0.1/32", "fd7a:115c:a1e0::1/128", "192.168.1.0/24"]
                .iter()
                .map(|r| r.parse().unwrap())
                .collect();

        // Without a self transport, everything goes to the netstack.
        let table = self_routes(&accepted, &tailnet_address, netstack, None);
        assert_eq!(lookup(&table, "100.64.0.1"), Some(netstack));
        assert_eq!(lookup(&table, "fd7a:115c:a1e0::1"), Some(netstack));
        assert_eq!(lookup(&table, "192.168.1.5"), Some(netstack));

        // With one, only our own addresses move to it.
        let table = self_routes(&accepted, &tailnet_address, netstack, Some(raw));
        assert_eq!(lookup(&table, "100.64.0.1"), Some(raw));
        assert_eq!(lookup(&table, "fd7a:115c:a1e0::1"), Some(raw));
        assert_eq!(lookup(&table, "192.168.1.5"), Some(netstack));
        assert_eq!(lookup(&table, "100.64.0.2"), None);
    }
}

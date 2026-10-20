//! Basic `tailscale` tests.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use tailscale::{Config, Device, Error, netstack::UdpSocket};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::timeout,
};

const NET_TIMEOUT: Duration = Duration::from_secs(1);
const RESEND_INTERVAL: Duration = Duration::from_millis(250);

#[tracing_test::traced_test]
#[tokio::test]
async fn ipv4_addr() {
    if !ts_test_util::run_net_tests() {
        tracing::warn!("net tests disabled");
        return;
    }

    timeout(NET_TIMEOUT, async move {
        let dev = make_ts_device().await.unwrap();
        let ip = dev.ipv4_addr().await.unwrap();
        assert!(!ip.is_unspecified());
    })
    .await
    .unwrap();
}

#[tracing_test::traced_test]
#[tokio::test]
async fn ipv6_addr() {
    if !ts_test_util::run_net_tests() {
        tracing::warn!("net tests disabled");
        return;
    }

    timeout(NET_TIMEOUT, async move {
        let dev = make_ts_device().await.unwrap();
        let ip = dev.ipv6_addr().await.unwrap();
        assert!(!ip.is_unspecified());
    })
    .await
    .unwrap();
}

#[tracing_test::traced_test]
#[tokio::test]
async fn tcp_listen4() {
    if !ts_test_util::run_net_tests() {
        tracing::warn!("net tests disabled");
        return;
    }

    timeout(NET_TIMEOUT, async move {
        let dev = make_ts_device().await.unwrap();

        let _listener = dev
            .tcp_listen((dev.ipv4_addr().await.unwrap(), 1234).into())
            .await
            .unwrap();
    })
    .await
    .unwrap();
}

#[tracing_test::traced_test]
#[tokio::test]
async fn tcp_listen6() {
    if !ts_test_util::run_net_tests() {
        tracing::warn!("net tests disabled");
        return;
    }

    timeout(NET_TIMEOUT, async move {
        let dev = make_ts_device().await.unwrap();

        let _listener = dev
            .tcp_listen((dev.ipv6_addr().await.unwrap(), 1234).into())
            .await
            .unwrap();
    })
    .await
    .unwrap();
}

#[tracing_test::traced_test]
#[tokio::test]
async fn tcp_connect4() {
    if !ts_test_util::run_net_tests() {
        tracing::warn!("net tests disabled");
        return;
    }

    timeout(NET_TIMEOUT, async move {
        let dev = make_ts_device().await.unwrap();
        let ip = dev.ipv4_addr().await.unwrap();

        test_tcp(&dev, (ip, 1234).into()).await;
    })
    .await
    .unwrap();
}

#[tracing_test::traced_test]
#[tokio::test]
async fn tcp_connect6() {
    if !ts_test_util::run_net_tests() {
        tracing::warn!("net tests disabled");
        return;
    }

    timeout(NET_TIMEOUT, async move {
        let dev = make_ts_device().await.unwrap();
        let ip = dev.ipv6_addr().await.unwrap();

        test_tcp(&dev, (ip, 1234).into()).await;
    })
    .await
    .unwrap();
}

async fn test_tcp(dev: &tailscale::Device, listen_addr: SocketAddr) {
    let listener = dev.tcp_listen(listen_addr).await.unwrap();

    let accept_task = tokio::spawn(async move { listener.accept().await });

    let mut conn = dev.tcp_connect(listen_addr).await.unwrap();
    let mut conn2 = accept_task.await.unwrap().unwrap();

    assert_eq!(conn.local_addr(), conn2.remote_addr());
    assert_eq!(conn2.local_addr(), conn.remote_addr());

    test_io_roundtrip(&mut conn, &mut conn2).await;
    test_io_roundtrip(&mut conn2, &mut conn).await;
}

async fn test_io_roundtrip(mut r: impl AsyncRead + Unpin, mut w: impl AsyncWrite + Unpin) {
    w.write_all(b"hello").await.unwrap();
    let mut b = [0u8; b"hello".len()];
    r.read_exact(&mut b).await.unwrap();
    assert_eq!(&b, b"hello");
}

#[tracing_test::traced_test]
#[tokio::test]
async fn udp4() {
    if !ts_test_util::run_net_tests() {
        tracing::warn!("net tests disabled");
        return;
    }

    timeout(NET_TIMEOUT, async move {
        let dev = make_ts_device().await.unwrap();
        let ip = dev.ipv4_addr().await.unwrap();
        test_udp(&dev, ip.into()).await;
    })
    .await
    .unwrap();
}

#[tracing_test::traced_test]
#[tokio::test]
async fn udp6() {
    if !ts_test_util::run_net_tests() {
        tracing::warn!("net tests disabled");
        return;
    }

    timeout(NET_TIMEOUT, async move {
        let dev = make_ts_device().await.unwrap();
        let ip = dev.ipv6_addr().await.unwrap();
        test_udp(&dev, ip.into()).await;
    })
    .await
    .unwrap();
}

async fn test_udp(dev: &tailscale::Device, ip: IpAddr) {
    let udp1 = dev.udp_bind((ip, 1234).into()).await.unwrap();
    let udp2 = dev.udp_bind((ip, 5678).into()).await.unwrap();

    test_udp_unidir(&udp1, &udp2).await;
    test_udp_unidir(&udp2, &udp1).await;
}

async fn test_udp_unidir(tx: &UdpSocket, rx: &UdpSocket) {
    tx.send_to(rx.local_addr(), b"hello").await.unwrap();

    let (who, msg) = rx.recv_from_bytes().await.unwrap();

    assert_eq!(who, tx.local_addr());
    assert_eq!(msg.as_ref(), b"hello");
}

#[tracing_test::traced_test]
#[tokio::test]
async fn raw_packets_udp4() {
    if !ts_test_util::run_net_tests() {
        tracing::warn!("net tests disabled");
        return;
    }

    // Two devices have to find each other through the tailnet, which takes longer than the
    // single-device tests.
    timeout(Duration::from_secs(30), async move {
        let dev = make_ts_device().await.unwrap();
        let raw_dev = Device::new(
            &Config {
                raw_packets: true,
                ..Default::default()
            },
            Some(ts_test_util::auth_key().unwrap()),
        )
        .await
        .unwrap();
        let mut raw = raw_dev.take_raw_packets().await.unwrap().unwrap();
        assert!(raw_dev.take_raw_packets().await.unwrap().is_none());

        let ip = dev.ipv4_addr().await.unwrap();
        let raw_ip = raw_dev.ipv4_addr().await.unwrap();
        let udp = Arc::new(dev.udp_bind((ip, 1234).into()).await.unwrap());

        // Packets sent while the tunnel between the two devices is coming up get dropped, so
        // keep resending until one arrives. Receives aren't wrapped in a timeout: the outer one
        // bounds the test.

        // Netstack -> raw: the packet arrives unmodified, addressed to the raw device.
        let sender = tokio::spawn({
            let udp = udp.clone();
            async move {
                loop {
                    udp.send_to((raw_ip, 5678).into(), b"hello").await.unwrap();
                    tokio::time::sleep(RESEND_INTERVAL).await;
                }
            }
        });
        let packet = raw.recv().await.unwrap();
        sender.abort();
        assert_eq!(
            parse_udp4(&packet),
            Some((ip, 1234, raw_ip, 5678, &b"hello"[..]))
        );

        // Raw -> netstack.
        let sender = tokio::spawn({
            let raw = raw.sender();
            async move {
                loop {
                    raw.send(&udp4_packet(raw_ip, 5678, ip, 1234, b"world"))
                        .unwrap();
                    tokio::time::sleep(RESEND_INTERVAL).await;
                }
            }
        });
        let (who, msg) = udp.recv_from_bytes().await.unwrap();
        sender.abort();
        assert_eq!(who, (raw_ip, 5678).into());
        assert_eq!(msg.as_ref(), b"world");
    })
    .await
    .unwrap();
}

/// Build an IPv4/UDP packet.
fn udp4_packet(src: Ipv4Addr, sport: u16, dst: Ipv4Addr, dport: u16, payload: &[u8]) -> Vec<u8> {
    let udp_len = 8 + payload.len() as u16;
    let total_len = 20 + udp_len;

    let mut packet = vec![0x45, 0, 0, 0, 0, 0, 0, 0, 64, 17, 0, 0];
    packet[2..4].copy_from_slice(&total_len.to_be_bytes());
    packet.extend_from_slice(&src.octets());
    packet.extend_from_slice(&dst.octets());
    let ip_checksum = checksum(&packet);
    packet[10..12].copy_from_slice(&ip_checksum.to_be_bytes());

    let mut udp = vec![];
    udp.extend_from_slice(&sport.to_be_bytes());
    udp.extend_from_slice(&dport.to_be_bytes());
    udp.extend_from_slice(&udp_len.to_be_bytes());
    udp.extend_from_slice(&[0, 0]);
    udp.extend_from_slice(payload);

    let mut pseudo = vec![];
    pseudo.extend_from_slice(&src.octets());
    pseudo.extend_from_slice(&dst.octets());
    pseudo.extend_from_slice(&[0, 17]);
    pseudo.extend_from_slice(&udp_len.to_be_bytes());
    pseudo.extend_from_slice(&udp);
    let udp_checksum = match checksum(&pseudo) {
        0 => 0xffff,
        c => c,
    };
    udp[6..8].copy_from_slice(&udp_checksum.to_be_bytes());

    packet.extend_from_slice(&udp);
    packet
}

/// Parse an IPv4/UDP packet into (src, sport, dst, dport, payload).
fn parse_udp4(packet: &[u8]) -> Option<(Ipv4Addr, u16, Ipv4Addr, u16, &[u8])> {
    let ihl = usize::from(*packet.first()? & 0x0f) * 4;
    if packet[0] >> 4 != 4 || *packet.get(9)? != 17 {
        return None;
    }

    let src = Ipv4Addr::from(<[u8; 4]>::try_from(packet.get(12..16)?).ok()?);
    let dst = Ipv4Addr::from(<[u8; 4]>::try_from(packet.get(16..20)?).ok()?);
    let udp = packet.get(ihl..)?;
    let sport = u16::from_be_bytes([*udp.first()?, *udp.get(1)?]);
    let dport = u16::from_be_bytes([*udp.get(2)?, *udp.get(3)?]);

    Some((src, sport, dst, dport, udp.get(8..)?))
}

/// The Internet checksum (RFC 1071).
fn checksum(data: &[u8]) -> u16 {
    let mut sum = data
        .chunks(2)
        .map(|c| u32::from(u16::from_be_bytes([c[0], *c.get(1).unwrap_or(&0)])))
        .sum::<u32>();
    while sum > 0xffff {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

async fn make_ts_device() -> Result<Device, Error> {
    Device::new(&Config::default(), Some(ts_test_util::auth_key().unwrap())).await
}

use std::sync::Arc;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::watch,
};
use wakemc::{
    backend::Backend,
    config::Config,
    protocol::{disconnect_packet, read_frame, read_varint, split_packet, status_response},
    proxy::handle_connection,
};

#[test]
fn varint_round_trip() {
    for value in [0, 1, 127, 128, 255, 2_097_151] {
        let mut encoded = Vec::new();
        let mut current = value;
        loop {
            let mut byte = (current & 0x7f) as u8;
            current >>= 7;
            if current != 0 {
                byte |= 0x80;
            }
            encoded.push(byte);
            if current == 0 {
                break;
            }
        }
        let mut offset = 0;
        assert_eq!(read_varint(&encoded, &mut offset).unwrap(), value);
    }
}

#[test]
fn status_packet_contains_motd() {
    assert!(String::from_utf8_lossy(&status_response("hello").unwrap()).contains("hello"));
    let escaped = status_response("line\r\n\"quoted\"").unwrap();
    assert!(String::from_utf8_lossy(&escaped).contains(r#"line\r\n\"quoted\""#));
}

#[test]
fn config_defaults_are_applied() {
    let config: Config = toml::from_str(
        r#"
        [backend]
        host = "127.0.0.1"
        port = 25566
        [startup]
        command = "echo start"
        [messages]
        "#,
    )
    .unwrap();
    assert_eq!(config.backend.probe_interval_ms, 5000);
    assert_eq!(
        config.messages.offline_motd,
        "Server offline - reconnect to wake it"
    );
    assert_eq!(
        config.messages.starting_motd,
        "§2Server is starting - please reconnect shortly"
    );
    assert_eq!(config.startup.timeout_seconds, 120);
}

#[tokio::test]
async fn invalid_frame_length_is_rejected() {
    let (mut writer, mut reader) = tokio::io::duplex(8);
    writer
        .write_all(&[0x80, 0x80, 0x80, 0x80, 0x08])
        .await
        .unwrap();
    let error = read_frame(&mut reader).await.unwrap_err();
    assert!(error.to_string().contains("invalid packet length"));
}

#[test]
fn packet_outputs_have_expected_ids_and_payloads() {
    let disconnect = disconnect_packet("starting").unwrap();
    let (packet_id, payload) = split_packet(&disconnect).unwrap();
    assert_eq!(packet_id, 0);
    assert!(String::from_utf8_lossy(payload).contains("starting"));

    let mut pong = vec![0x03, 0x01, 0xaa, 0xbb];
    let payload = &pong.split_off(2);
    let response = wakemc::protocol::pong_response(payload);
    let (packet_id, response_payload) = split_packet(&response).unwrap();
    assert_eq!(packet_id, 1);
    assert_eq!(response_payload, payload);
}

#[tokio::test]
async fn offline_status_flow_returns_status_packet() {
    let config: Config = toml::from_str(
        r#"
        listen = "127.0.0.1:0"
        [backend]
        host = "127.0.0.1"
        port = 1
        probe_interval_ms = 10
        [startup]
        command = "true"
        timeout_seconds = 1
        [messages]
        offline_motd = "offline-test"
        "#,
    )
    .unwrap();
    let backend = Arc::new(Backend::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, _) = watch::channel(false);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, backend, shutdown.subscribe())
            .await
            .unwrap();
    });

    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&handshake_packet(1)).await.unwrap();
    client.write_all(&status_request()).await.unwrap();
    let response = read_frame(&mut client).await.unwrap();
    assert!(String::from_utf8_lossy(&response).contains("offline-test"));
    let _ = client.shutdown().await;
    server.await.unwrap();
}

#[tokio::test]
async fn shutdown_signal_closes_active_connection() {
    let config: Config = toml::from_str(
        r#"
        [backend]
        host = "127.0.0.1"
        port = 25566
        [startup]
        command = "true"
        [messages]
        "#,
    )
    .unwrap();
    let backend = Arc::new(Backend::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, shutdown) = watch::channel(false);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, backend, shutdown).await
    });
    let _client = TcpStream::connect(address).await.unwrap();
    sender.send(true).unwrap();
    assert!(server.await.unwrap().is_ok());
}

#[tokio::test]
async fn concurrent_probes_share_one_refresh() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let config: Config = toml::from_str(&format!(
        r#"
        [backend]
        host = "127.0.0.1"
        port = {}
        probe_interval_ms = 1000
        [startup]
        command = "true"
        [messages]
        "#,
        address.port()
    ))
    .unwrap();
    let backend = Arc::new(Backend::new(config));
    let accept = tokio::spawn(async move {
        let (_, _) = listener.accept().await.unwrap();
    });
    let (first, second) = tokio::join!(backend.probe(), backend.probe());
    assert!(first);
    assert!(second);
    tokio::time::timeout(std::time::Duration::from_secs(1), accept)
        .await
        .unwrap()
        .unwrap();
}

fn handshake_packet(next_state: i32) -> Vec<u8> {
    let mut body = vec![0, 0, 1, 0, 0, 0];
    body.push(next_state as u8);
    frame(body)
}

fn status_request() -> Vec<u8> {
    frame(vec![0])
}

fn frame(body: Vec<u8>) -> Vec<u8> {
    let mut packet = vec![body.len() as u8];
    packet.extend(body);
    packet
}

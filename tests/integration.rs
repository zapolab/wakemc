use std::sync::Arc;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::watch,
    time::{Duration, timeout},
};
use wakemc::{
    backend::Backend,
    config::Config,
    error::ProxyError,
    protocol::{
        disconnect_packet, read_frame, read_varint, read_varint_from, split_packet, status_response,
    },
    proxy::handle_connection,
};

#[test]
fn varint_round_trip() {
    for (encoded, value) in [
        (&[0x00][..], 0),
        (&[0x01][..], 1),
        (&[0x7f][..], 127),
        (&[0x80, 0x01][..], 128),
        (&[0xff, 0x01][..], 255),
        (&[0xff, 0xff, 0x7f][..], 2_097_151),
    ] {
        let mut offset = 0;
        assert_eq!(read_varint(encoded, &mut offset).unwrap(), value);
    }
}

#[tokio::test]
async fn async_varint_decoding_matches_buffer_decoding() {
    let encoded = [0xff, 0x01];
    let (mut writer, mut reader) = tokio::io::duplex(encoded.len());
    writer.write_all(&encoded).await.unwrap();
    let mut consumed = Vec::new();

    assert_eq!(
        read_varint_from(&mut reader, &mut consumed).await.unwrap(),
        255
    );
    assert_eq!(consumed, encoded);
}

#[test]
fn malformed_varints_are_rejected() {
    for encoded in [
        &[0x80, 0x80, 0x80, 0x80, 0x10][..],
        &[0x80, 0x80, 0x80, 0x80, 0x80][..],
    ] {
        let mut offset = 0;
        assert!(read_varint(encoded, &mut offset).is_err());
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

#[test]
fn expected_disconnect_errors_are_identified() {
    for kind in [
        std::io::ErrorKind::UnexpectedEof,
        std::io::ErrorKind::ConnectionReset,
        std::io::ErrorKind::BrokenPipe,
        std::io::ErrorKind::NotConnected,
    ] {
        assert!(ProxyError::from(std::io::Error::from(kind)).is_expected_disconnect());
    }
}

#[test]
fn protocol_errors_are_not_identified_as_disconnects() {
    assert!(!ProxyError::Protocol("invalid packet".into()).is_expected_disconnect());
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
    let mut config = test_config(1);
    config.listen = "127.0.0.1:0".into();
    config.backend.probe_interval_ms = 10;
    config.startup.timeout_seconds = 1;
    config.messages.offline_motd = "offline-test".into();
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
async fn invalid_status_packet_id_is_rejected() {
    let backend = Arc::new(Backend::new(test_config(1)));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (_shutdown_sender, shutdown) = watch::channel(false);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, backend, shutdown).await
    });

    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&handshake_packet(1)).await.unwrap();
    client.write_all(&frame(vec![1])).await.unwrap();

    let error = server.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("expected status request packet"));
}

#[tokio::test]
async fn invalid_ping_packet_id_is_rejected() {
    let mut config = test_config(1);
    config.messages.offline_motd = "offline".into();
    let backend = Arc::new(Backend::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (_shutdown_sender, shutdown) = watch::channel(false);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, backend, shutdown).await
    });

    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&handshake_packet(1)).await.unwrap();
    client.write_all(&status_request()).await.unwrap();
    read_frame(&mut client).await.unwrap();
    client.write_all(&frame(vec![2])).await.unwrap();

    let error = server.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("expected ping packet"));
}

#[tokio::test]
async fn malformed_ping_packet_is_rejected() {
    let backend = Arc::new(Backend::new(test_config(1)));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (_shutdown_sender, shutdown) = watch::channel(false);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, backend, shutdown).await
    });

    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&handshake_packet(1)).await.unwrap();
    client.write_all(&status_request()).await.unwrap();
    read_frame(&mut client).await.unwrap();
    client.write_all(&[1, 0x80]).await.unwrap();

    let error = server.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("truncated VarInt"));
}

#[tokio::test]
async fn shutdown_signal_closes_active_connection() {
    let config = test_config(25566);
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
async fn client_disconnect_does_not_mark_backend_offline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_address = listener.local_addr().unwrap();
    let config = test_config(backend_address.port());
    let backend = Arc::new(Backend::new(config));
    backend.mark_online().await;
    let backend_task = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy_listener.local_addr().unwrap();
    let (shutdown, _) = watch::channel(false);
    let proxy_backend = Arc::clone(&backend);
    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener.accept().await.unwrap();
        handle_connection(stream, proxy_backend, shutdown.subscribe()).await
    });

    let mut client = TcpStream::connect(proxy_address).await.unwrap();
    client.write_all(&handshake_packet(2)).await.unwrap();
    client.write_all(&frame(vec![0])).await.unwrap();
    client.shutdown().await.unwrap();

    assert!(proxy_task.await.unwrap().is_ok());
    assert_eq!(backend.state().await, wakemc::backend::BackendState::Online);
    backend_task.abort();
}

#[tokio::test]
async fn backend_disconnect_finishes_forwarding_task() {
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_port = backend_listener.local_addr().unwrap().port();
    let backend_task = tokio::spawn(async move {
        let (mut stream, _) = backend_listener.accept().await.unwrap();
        read_frame(&mut stream).await.unwrap();
        read_frame(&mut stream).await.unwrap();
        drop(stream);
    });
    let backend = Arc::new(Backend::new(test_config(backend_port)));
    backend.mark_online().await;

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy_listener.local_addr().unwrap();
    let (_shutdown_sender, shutdown) = watch::channel(false);
    let proxy_backend = Arc::clone(&backend);
    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener.accept().await.unwrap();
        handle_connection(stream, proxy_backend, shutdown).await
    });

    let mut client = TcpStream::connect(proxy_address).await.unwrap();
    client.write_all(&handshake_packet(2)).await.unwrap();
    client.write_all(&frame(vec![0])).await.unwrap();

    assert!(
        timeout(Duration::from_secs(1), proxy_task)
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );
    backend_task.await.unwrap();
}

#[tokio::test]
async fn refresh_detects_backend_that_went_offline() {
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_port = backend_listener.local_addr().unwrap().port();
    drop(backend_listener);
    let mut config = test_config(backend_port);
    config.backend.probe_interval_ms = 10;
    config.messages.offline_motd = "backend-offline".into();
    let backend = Arc::new(Backend::new(config));
    backend.mark_online().await;

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy_listener.local_addr().unwrap();
    let (shutdown, _) = watch::channel(false);
    let proxy_backend = Arc::clone(&backend);
    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener.accept().await.unwrap();
        handle_connection(stream, proxy_backend, shutdown.subscribe()).await
    });

    let mut client = TcpStream::connect(proxy_address).await.unwrap();
    client.write_all(&handshake_packet(1)).await.unwrap();
    client.write_all(&status_request()).await.unwrap();
    let response = read_frame(&mut client).await.unwrap();

    assert!(String::from_utf8_lossy(&response).contains("backend-offline"));
    assert_eq!(
        backend.state().await,
        wakemc::backend::BackendState::Offline
    );
    let _ = client.shutdown().await;
    proxy_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn concurrent_probes_share_one_refresh() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = test_config(address.port());
    config.backend.probe_interval_ms = 1000;
    let backend = Arc::new(Backend::new(config));
    let accept = tokio::spawn(async move {
        let mut connections = 0;
        while let Ok(Ok((stream, _))) = timeout(Duration::from_millis(100), listener.accept()).await
        {
            connections += 1;
            drop(stream);
        }
        connections
    });
    let (first, second) = tokio::join!(backend.probe(), backend.probe());
    assert!(first);
    assert!(second);
    assert_eq!(
        timeout(Duration::from_secs(1), accept)
            .await
            .unwrap()
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn startup_timeout_returns_backend_to_offline() {
    let mut config = test_config(1);
    config.backend.probe_interval_ms = 10;
    config.startup.command = "sleep 5".into();
    config.startup.timeout_seconds = 1;
    let backend = Arc::new(Backend::new(config));
    backend.ensure_starting().await;
    assert_eq!(
        backend.state().await,
        wakemc::backend::BackendState::Offline
    );
}

#[tokio::test]
async fn login_marks_backend_starting_before_returning() {
    let mut config = test_config(1);
    config.backend.probe_interval_ms = 10;
    config.startup.command = "sleep 0.1".into();
    config.startup.timeout_seconds = 1;
    let backend = Arc::new(Backend::new(config));

    backend.request_starting().await;
    assert_eq!(
        backend.state().await,
        wakemc::backend::BackendState::Starting
    );
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

fn test_config(backend_port: u16) -> Config {
    toml::from_str(&format!(
        r#"
        [backend]
        host = "127.0.0.1"
        port = {backend_port}
        [startup]
        command = "true"
        [messages]
        "#
    ))
    .unwrap()
}

use crate::{
    backend::{Backend, BackendState},
    error::ProxyError,
    protocol::{
        disconnect_packet, pong_response, read_frame, read_varint, split_packet, status_response,
    },
};
use std::{io, sync::Arc};
use tokio::{
    io::{AsyncWriteExt, copy},
    net::TcpStream,
    sync::watch,
};

pub async fn handle_connection(
    mut client: TcpStream,
    backend: Arc<Backend>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ProxyError> {
    tokio::select! {
        result = handle_connection_inner(&mut client, Arc::clone(&backend)) => result,
        _ = shutdown.changed() => Ok(()),
    }
}

async fn handle_connection_inner(
    client: &mut TcpStream,
    backend: Arc<Backend>,
) -> Result<(), ProxyError> {
    let handshake = read_frame(client).await?;
    let (packet_id, payload) = split_packet(&handshake)?;
    if packet_id != 0 {
        return Err(ProxyError::Protocol("expected handshake packet".into()));
    }
    let mut offset = 0;
    read_varint(payload, &mut offset)?;
    let address_length = read_varint(payload, &mut offset)?;
    if address_length < 0 || offset + address_length as usize + 2 > payload.len() {
        return Err(ProxyError::Protocol("truncated handshake packet".into()));
    }
    offset += address_length as usize + 2;
    let next_state = read_varint(payload, &mut offset)?;

    let mut state = backend.state().await;
    if state == BackendState::Online {
        match connect_and_forward(client, &handshake, None, &backend).await {
            Ok(()) => return Ok(()),
            Err(ForwardError::Backend(error)) => {
                log_backend_failure(&error);
                backend.mark_offline().await;
                state = BackendState::Offline;
            }
            Err(ForwardError::Client) => return Ok(()),
        }
    }
    match next_state {
        1 => {
            let request = read_frame(client).await?;
            let (request_id, _) = split_packet(&request)?;
            if request_id == 0 {
                if state == BackendState::Offline && backend.probe().await {
                    backend.mark_online().await;
                    match connect_and_forward(client, &handshake, Some(&request), &backend).await {
                        Ok(()) => return Ok(()),
                        Err(ForwardError::Backend(error)) => {
                            log_backend_failure(&error);
                            backend.mark_offline().await;
                        }
                        Err(ForwardError::Client) => return Ok(()),
                    }
                }
                let motd = match backend.state().await {
                    BackendState::Starting => &backend.config.messages.starting_motd,
                    _ => &backend.config.messages.offline_motd,
                };
                client.write_all(&status_response(motd)?).await?;
                if let Ok(ping) = read_frame(client).await {
                    let (ping_id, payload) = split_packet(&ping)?;
                    if ping_id == 1 {
                        client.write_all(&pong_response(payload)).await?;
                    }
                }
            }
        }
        2 => {
            read_frame(client).await?;
            if state == BackendState::Offline {
                let backend = Arc::clone(&backend);
                tokio::spawn(async move {
                    backend.ensure_starting().await;
                });
            }
            client
                .write_all(&disconnect_packet(&backend.config.messages.kick)?)
                .await?;
        }
        _ => {}
    }
    Ok(())
}

enum ForwardError {
    Client,
    Backend(io::Error),
}

fn log_backend_failure(error: &io::Error) {
    if !matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    ) {
        eprintln!("backend connection failed, marking it offline: {error}");
    }
}

async fn connect_and_forward(
    client: &mut TcpStream,
    handshake: &[u8],
    request: Option<&[u8]>,
    backend: &Backend,
) -> Result<(), ForwardError> {
    let mut upstream = TcpStream::connect(backend.config.backend.address())
        .await
        .map_err(ForwardError::Backend)?;
    upstream
        .write_all(handshake)
        .await
        .map_err(ForwardError::Backend)?;
    if let Some(request) = request {
        upstream
            .write_all(request)
            .await
            .map_err(ForwardError::Backend)?;
    }

    let (mut client_read, mut client_write) = client.split();
    let (mut backend_read, mut backend_write) = upstream.split();
    let client_to_backend = copy(&mut client_read, &mut backend_write);
    let backend_to_client = copy(&mut backend_read, &mut client_write);
    tokio::pin!(client_to_backend);
    tokio::pin!(backend_to_client);

    tokio::select! {
        result = &mut client_to_backend => result.map(|_| ()).map_err(ForwardError::Backend),
        result = &mut backend_to_client => result.map(|_| ()).map_err(|_| ForwardError::Client),
    }
}

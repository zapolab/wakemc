use std::io;
use thiserror::Error;

#[derive(Debug)]
pub enum ForwardError {
    Client,
    Backend(io::Error),
}

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ProxyError {
    pub fn is_expected_disconnect(&self) -> bool {
        matches!(
            self,
            Self::Io(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::NotConnected
                )
        )
    }
}

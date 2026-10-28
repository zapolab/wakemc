use std::io;
use thiserror::Error;

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

#[cfg(test)]
mod tests {
    use super::ProxyError;
    use std::io;

    #[test]
    fn expected_disconnect_errors_are_identified() {
        for kind in [
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::NotConnected,
        ] {
            assert!(ProxyError::from(io::Error::from(kind)).is_expected_disconnect());
        }
    }

    #[test]
    fn protocol_errors_are_not_identified_as_disconnects() {
        assert!(!ProxyError::Protocol("invalid packet".into()).is_expected_disconnect());
    }
}

use wakemc::{
    config::Config,
    protocol::{read_varint, status_response},
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

use crate::error::ProxyError;
use tokio::io::{AsyncRead, AsyncReadExt};

pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>, ProxyError> {
    let mut encoded = Vec::new();
    let length = read_varint_from(reader, &mut encoded).await?;
    if !(0..=2 * 1024 * 1024).contains(&length) {
        return Err(ProxyError::Protocol("invalid packet length".into()));
    }
    let body_start = encoded.len();
    encoded.resize(body_start + length as usize, 0);
    reader.read_exact(&mut encoded[body_start..]).await?;
    Ok(encoded)
}

pub async fn read_varint_from<R: AsyncRead + Unpin>(
    reader: &mut R,
    encoded: &mut Vec<u8>,
) -> Result<i32, ProxyError> {
    let mut value = 0;
    for index in 0..5 {
        let byte = reader.read_u8().await?;
        encoded.push(byte);
        if decode_varint_byte(byte, index, &mut value)? {
            return Ok(value);
        }
    }
    Err(ProxyError::Protocol("invalid VarInt".into()))
}

pub fn read_varint(data: &[u8], offset: &mut usize) -> Result<i32, ProxyError> {
    let mut value = 0;
    for index in 0..5 {
        let byte = *data
            .get(*offset)
            .ok_or_else(|| ProxyError::Protocol("truncated VarInt".into()))?;
        *offset += 1;
        if decode_varint_byte(byte, index, &mut value)? {
            return Ok(value);
        }
    }
    Err(ProxyError::Protocol("invalid VarInt".into()))
}

pub fn split_packet(frame: &[u8]) -> Result<(i32, &[u8]), ProxyError> {
    let mut offset = 0;
    read_varint(frame, &mut offset)?;
    let id = read_varint(frame, &mut offset)?;
    Ok((id, &frame[offset..]))
}

pub fn status_response(motd: &str) -> Result<Vec<u8>, ProxyError> {
    let json = format!(
        r#"{{"version":{{"name":"wakemc","protocol":769}},"players":{{"max":0,"online":0,"sample":[]}},"description":{{"text":{}}}}}"#,
        serde_json::to_string(motd).map_err(|error| ProxyError::Protocol(error.to_string()))?
    );
    string_packet(0, &json)
}

pub fn disconnect_packet(message: &str) -> Result<Vec<u8>, ProxyError> {
    let json = text_component(message)?;
    string_packet(0, &json)
}

pub fn pong_response(payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(payload.len() + 1);
    write_varint(1, &mut body);
    body.extend_from_slice(payload);
    packet(body)
}

fn write_varint(mut value: i32, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn decode_varint_byte(byte: u8, index: usize, value: &mut i32) -> Result<bool, ProxyError> {
    if index == 4 && byte & 0x70 != 0 {
        return Err(ProxyError::Protocol("invalid VarInt".into()));
    }
    *value |= ((byte & 0x7f) as i32) << (index * 7);
    Ok(byte & 0x80 == 0)
}

fn packet(body: Vec<u8>) -> Vec<u8> {
    let mut result = Vec::new();
    write_varint(body.len() as i32, &mut result);
    result.extend(body);
    result
}

fn text_component(message: &str) -> Result<String, ProxyError> {
    Ok(format!(
        r#"{{"text":{}}}"#,
        serde_json::to_string(message).map_err(|error| ProxyError::Protocol(error.to_string()))?
    ))
}

fn string_packet(id: i32, value: &str) -> Result<Vec<u8>, ProxyError> {
    let mut body = Vec::new();
    write_varint(id, &mut body);
    write_string(value, &mut body);
    Ok(packet(body))
}

fn write_string(value: &str, output: &mut Vec<u8>) {
    write_varint(value.len() as i32, output);
    output.extend_from_slice(value.as_bytes());
}

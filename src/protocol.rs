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
    for _ in 0..5 {
        let byte = reader.read_u8().await?;
        encoded.push(byte);
        if byte & 0x80 == 0 {
            return decode_varint_bytes(encoded);
        }
    }
    decode_varint_bytes(encoded)
}

pub fn read_varint(data: &[u8], offset: &mut usize) -> Result<i32, ProxyError> {
    let start = *offset;
    for _ in 0..5 {
        let byte = *data
            .get(*offset)
            .ok_or_else(|| ProxyError::Protocol("truncated VarInt".into()))?;
        *offset += 1;
        if byte & 0x80 == 0 {
            return decode_varint_bytes(&data[start..*offset]);
        }
    }
    decode_varint_bytes(&data[start..*offset])
}

pub fn split_packet(frame: &[u8]) -> Result<(i32, &[u8]), ProxyError> {
    let mut offset = 0;
    read_varint(frame, &mut offset)?;
    let id = read_varint(frame, &mut offset)?;
    Ok((id, &frame[offset..]))
}

pub fn status_response(motd: &str) -> Result<Vec<u8>, ProxyError> {
    let response = serde_json::json!({
        "version": {"name": "wakemc", "protocol": 769},
        "players": {"max": 0, "online": 0, "sample": []},
        "description": {"text": motd},
    });
    let json = serde_json::to_string(&response)
        .map_err(|error| ProxyError::Protocol(error.to_string()))?;
    Ok(string_packet(0, &json))
}

pub fn disconnect_packet(message: &str) -> Result<Vec<u8>, ProxyError> {
    let json = text_component(message)?;
    Ok(string_packet(0, &json))
}

pub fn pong_response(payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(payload.len() + 1);
    write_varint(1, &mut body);
    body.extend_from_slice(payload);
    packet(body)
}

fn write_varint(value: i32, output: &mut Vec<u8>) {
    let mut value = value as u32;
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

fn decode_varint_bytes(data: &[u8]) -> Result<i32, ProxyError> {
    let mut value = 0;
    for (index, &byte) in data.iter().enumerate() {
        if index == 4 && byte & 0x70 != 0 {
            return Err(ProxyError::Protocol("invalid VarInt".into()));
        }
        value |= ((byte & 0x7f) as i32) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(ProxyError::Protocol("invalid VarInt".into()))
}

fn packet(body: Vec<u8>) -> Vec<u8> {
    let mut result = Vec::new();
    write_varint(body.len() as i32, &mut result);
    result.extend(body);
    result
}

fn text_component(message: &str) -> Result<String, ProxyError> {
    serde_json::to_string(&serde_json::json!({"text": message}))
        .map_err(|error| ProxyError::Protocol(error.to_string()))
}

fn string_packet(id: i32, value: &str) -> Vec<u8> {
    let mut body = Vec::new();
    write_varint(id, &mut body);
    write_string(value, &mut body);
    packet(body)
}

fn write_string(value: &str, output: &mut Vec<u8>) {
    write_varint(value.len() as i32, output);
    output.extend_from_slice(value.as_bytes());
}

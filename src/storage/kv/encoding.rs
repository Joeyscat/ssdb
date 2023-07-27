//! Order-preserving encodings for use in keys.
//!
//! bool:    0x00 为真, 0x01 为假.
//! Vec<u8>: 0x00 使用 0x00 0xff 转义, 0x00 0x00 用作终止符.
//! String:  参考 Vec<u8>.
//! u64:     大端二进制表示.
//! i64:     大端二进制表示, 符号位翻转.
//! f64:     大端二进制表示, 正数符号位翻转, 负数符号位和所有位翻转.
//! Value:   参考上面, 但是带有类型前缀 0x00=Null, 0x01=Boolean, 0x02=Float, 0x03=Integer, 0x04=String

use crate::error::{Error, Result};
use crate::sql::types::Value;

use std::convert::TryInto;

pub fn encode_boolean(b: bool) -> u8 {
    if b {
        0x01
    } else {
        0x00
    }
}

pub fn decode_boolean(b: u8) -> Result<bool> {
    match b {
        0x00 => Ok(false),
        0x01 => Ok(true),
        _ => Err(Error::Internal(format!("Invalid boolean encoding: {}", b))),
    }
}

/// 从切片中解码布尔值并缩小切片
pub fn take_boolean(bytes: &mut &[u8]) -> Result<bool> {
    take_byte(bytes).and_then(decode_boolean)
}

/// 对字节向量进行编码。 `0x00` 转义为 `0x00 0xff`, `0x00 0x00` 用作终止符
/// 参考: https://activesphere.com/blog/2018/08/17/order-preserving-serialization
pub fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 2);
    out.extend(
        bytes
            .iter()
            .flat_map(|b| match b {
                0x00 => vec![0x00, 0xff],
                b => vec![*b],
            })
            .chain(vec![0x00, 0x00]),
    );

    out
}

/// 从切片中取出一个字节并缩短它，不进行任何转义
pub fn take_byte(bytes: &mut &[u8]) -> Result<u8> {
    if bytes.is_empty() {
        return Err(Error::Internal("Unexpected end of bytes".into()));
    }
    let b = bytes[0];
    *bytes = &bytes[1..];
    Ok(b)
}

/// 从切片中解码字节向量并缩短它
pub fn take_bytes(bytes: &mut &[u8]) -> Result<Vec<u8>> {
    let mut decoded = Vec::with_capacity(bytes.len() / 2);
    let mut iter = bytes.iter().enumerate();
    let taken = loop {
        match iter.next().map(|(_, b)| b) {
            Some(0x00) => match iter.next() {
                Some((i, 0x00)) => break i + 1,        // 0x00 0x00 为终止符
                Some((_, 0xff)) => decoded.push(0x00), // 0x00 0xff 转义为 0x00
                Some((_, b)) => return Err(Error::Value(format!("Invalid byte encoding: {}", b))),
                None => return Err(Error::Value("Unexpected end of bytes".into())),
            },
            Some(b) => decoded.push(*b),
            None => return Err(Error::Value("Unexpected end of bytes".into())),
        }
    };
    *bytes = &bytes[taken..];
    Ok(decoded)
}

/// 编码为 f64, 使用大端形式, 如果符号位为 0, 则将其翻转为 1, 否则翻转所有位.
/// 保留了自然的数字顺序, 末尾带有 NaN
pub fn encode_f64(f: f64) -> [u8; 8] {
    let mut bytes = f.to_be_bytes();
    if bytes[0] >> 7 & 1 == 0 {
        bytes[0] |= 1 << 7;
    } else {
        bytes.iter_mut().for_each(|b| *b = !*b);
    }
    bytes
}

pub fn decode_f64(mut bytes: [u8; 8]) -> f64 {
    if bytes[0] >> 7 & 1 == 1 {
        bytes[0] ^= 1 << 7;
    } else {
        bytes.iter_mut().for_each(|b| *b = !*b);
    }
    f64::from_be_bytes(bytes)
}

pub fn take_f64(bytes: &mut &[u8]) -> Result<f64> {
    if bytes.len() < 8 {
        return Err(Error::Internal(format!(
            "Unable to decode f64 from {} bytes",
            bytes.len()
        )));
    }
    let n = decode_f64(bytes[0..8].try_into()?);
    *bytes = &bytes[8..];
    Ok(n)
}

pub fn encode_i64(n: i64) -> [u8; 8] {
    let mut bytes = n.to_be_bytes();
    bytes[0] ^= 1 << 7; // 翻转符号位
    bytes
}

pub fn decode_i64(mut bytes: [u8; 8]) -> i64 {
    bytes[0] ^= 1 << 7; // 翻转符号位
    i64::from_be_bytes(bytes)
}

pub fn take_i64(bytes: &mut &[u8]) -> Result<i64> {
    if bytes.len() < 8 {
        return Err(Error::Internal(format!(
            "Unable to decode i64 from {} bytes",
            bytes.len()
        )));
    }
    let n = decode_i64(bytes[0..8].try_into()?);
    *bytes = &bytes[8..];
    Ok(n)
}

pub fn encode_u64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

pub fn decode_u64(bytes: [u8; 8]) -> u64 {
    u64::from_be_bytes(bytes)
}

pub fn take_u64(bytes: &mut &[u8]) -> Result<u64> {
    if bytes.len() < 8 {
        return Err(Error::Internal(format!(
            "Unable to decode u64 from {} bytes",
            bytes.len()
        )));
    }
    let n = decode_u64(bytes[0..8].try_into()?);
    *bytes = &bytes[8..];
    Ok(n)
}

pub fn encode_string(s: &str) -> Vec<u8> {
    encode_bytes(s.as_bytes())
}

pub fn take_string(bytes: &mut &[u8]) -> Result<String> {
    Ok(String::from_utf8(take_bytes(bytes)?)?)
}

pub fn encode_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![0x00],
        Value::Boolean(b) => vec![0x01, encode_boolean(*b)],
        Value::Float(f) => [&[0x02][..], &encode_f64(*f)].concat(),
        Value::Integer(i) => [&[0x03][..], &encode_i64(*i)].concat(),
        Value::String(s) => [&[0x04][..], &encode_string(s)].concat(),
    }
}

pub fn take_value(bytes: &mut &[u8]) -> Result<Value> {
    match take_byte(bytes)? {
        0x00 => Ok(Value::Null),
        0x01 => Ok(Value::Boolean(take_boolean(bytes)?)),
        0x02 => Ok(Value::Float(take_f64(bytes)?)),
        0x03 => Ok(Value::Integer(take_i64(bytes)?)),
        0x04 => Ok(Value::String(take_string(bytes)?)),
        b => Err(Error::Internal(format!("Invalid value prefix: {:x?}", b))),
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_boolean() ->Result<()> {
        unimplemented!()    
    }
}
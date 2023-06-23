use crate::util::read_varint;
use anyhow::{bail, Result};
use std::{fmt, str};

#[derive(Debug, Clone)]
pub enum RecordField {
    Null,
    Int8(i8),
    Int16(i16),
    Int24(i32),
    Int32(i32),
    Int48(i64),
    Int64(i64),
    Float64(f64),
    Zero,
    One,
    Internal,
    Blob(String),
    Text(String),
}

impl RecordField {
    pub fn parse_from_bytes(variant_indicator: u64, content: &[u8]) -> Result<(Self, &[u8])> {
        match variant_indicator {
            0 => Ok((Self::Null, content)),
            1 => {
                let num = i8::from_be_bytes([content[0]]);
                Ok((Self::Int8(num.into()), &content[1..]))
            }
            2 => {
                let num = i16::from_be_bytes(content[..2].try_into()?);
                Ok((Self::Int16(num.into()), &content[2..]))
            }
            3 => {
                let num = i32::from_be_bytes([0, content[0], content[1], content[2]]);
                Ok((Self::Int24(num.into()), &content[3..]))
            }
            4 => {
                let num = i32::from_be_bytes(content[..4].try_into()?);
                Ok((Self::Int32(num.into()), &content[4..]))
            }
            5 => {
                let bytes = [&[0, 0], &content[..6]].concat();
                let num = i64::from_be_bytes(bytes.try_into().unwrap()); // TODO: figure out why ? cannot be used here
                Ok((Self::Int48(num.into()), &content[6..]))
            }
            6 => {
                let num = i64::from_be_bytes(content[..8].try_into()?);
                Ok((Self::Int64(num.into()), &content[8..]))
            }
            7 => {
                let num = f64::from_be_bytes(content[..8].try_into()?);
                Ok((Self::Float64(num.into()), &content[8..]))
            }
            8 => Ok((Self::Zero, content)),
            9 => Ok((Self::One, content)),
            10 | 11 => Ok((Self::Internal, content)), // TODO: Should raise warning
            v @ 12.. if variant_indicator % 2 == 0 => {
                let blob_size = ((v - 12) / 2) as usize;
                let text = str::from_utf8(&content[..blob_size])?.to_owned();
                Ok((Self::Blob(text), &content[blob_size..]))
            }
            v @ 12.. if variant_indicator % 2 == 1 => {
                let text_size = ((v - 13) / 2) as usize;
                let text = str::from_utf8(&content[..text_size])?.to_owned();
                Ok((Self::Text(text), &content[text_size..]))
            }
            _ => {
                bail!("Not reachable")
            }
        }
    }
}

impl fmt::Display for RecordField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Int8(num) => write!(f, "{}", num),
            Self::Int16(num) => write!(f, "{}", num),
            Self::Int24(num) => write!(f, "{}", num),
            Self::Int32(num) => write!(f, "{}", num),
            Self::Int48(num) => write!(f, "{}", num),
            Self::Int64(num) => write!(f, "{}", num),
            Self::Float64(num) => write!(f, "{}", num),
            Self::Zero => write!(f, "ZERO"),
            Self::One => write!(f, "ONE"),
            Self::Internal => write!(f, "INTERNAL"),
            Self::Blob(text) => write!(f, "{}", text),
            Self::Text(text) => write!(f, "{}", text),
        }
    }
}

pub fn parse_records(payload: &[u8]) -> Result<Vec<RecordField>> {
    let (header_size, _rest) = read_varint(payload)?;

    let header = &payload[..header_size as usize];
    let (header_size, mut header) = read_varint(header)?; // consume and skip the first varint again TODO: understand mut header, &mut header

    let mut content = &payload[header_size as usize..];

    let mut parsed_records = Vec::new();
    while header.len() > 0 {
        let (variant_indicator, header_left) = read_varint(header)?;
        header = header_left;
        let (serial_type, content_left) =
            RecordField::parse_from_bytes(variant_indicator, content)?;
        content = content_left;
        parsed_records.push(serial_type);
    }
    Ok(parsed_records)
}

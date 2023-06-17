use crate::util::read_varint;
use anyhow::{bail, Result};
use std::str;

#[derive(Debug)]
pub struct SqliteSchema {
    pub schema_type: String,
    pub name: String,
    pub tbl_name: String,
    pub rootpage: u64,
    pub sql: String,
}

impl SqliteSchema {
    pub fn from_bytes(payload: &[u8]) -> Result<Self> {
        let (header_size, _rest) = read_varint(payload)?;

        let header = &payload[..header_size as usize];
        let (header_size, header) = read_varint(header)?; // consume and skip the first varint again

        let mut content = &payload[header_size as usize..];

        let (schema_type_type, header) = read_varint(header)?;
        let schema_type_size = (schema_type_type - 13) / 2;
        let schema_type = str::from_utf8(&content[..schema_type_size as usize])?.to_owned();
        content = &content[schema_type_size as usize..];

        let (name_type, header) = read_varint(header)?;
        let name_size = (name_type - 13) / 2;
        let name = str::from_utf8(&content[..name_size as usize])?.to_owned();
        content = &content[name_size as usize..];

        let (tbl_name_type, header) = read_varint(header)?;
        let tbl_name_size = (tbl_name_type - 13) / 2;
        let tbl_name = str::from_utf8(&content[..tbl_name_size as usize])?.to_owned();
        content = &content[tbl_name_size as usize..];

        let (rootpage_type, header) = read_varint(header)?;
        let rootpage_size = match rootpage_type {
            1 => 1,
            2 => 2,
            3 => 3,
            4 => 4,
            5 => 6,
            6 => 8,
            _ => bail!(
                "Expected int type (1..6) for rootpage, observed {}",
                rootpage_type
            ),
        };
        let mut rootpage_bytes: [u8; 8] = [0; 8];
        for (i, byte) in &mut content[..rootpage_size as usize].iter().rev().enumerate() {
            rootpage_bytes[rootpage_bytes.len() - i - 1] = byte.to_owned();
        }
        let rootpage = u64::from_be_bytes(rootpage_bytes);
        content = &content[rootpage_size as usize..];

        let (sql_type, _header) = read_varint(header)?;
        let sql_size = (sql_type - 13) / 2;
        let sql = str::from_utf8(&content[..sql_size as usize])?.to_owned();

        Ok(Self {
            schema_type,
            name,
            tbl_name,
            rootpage,
            sql,
        })
    }
}

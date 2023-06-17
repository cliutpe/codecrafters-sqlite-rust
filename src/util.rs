use crate::sqlite_schema::SqliteSchema;
use anyhow;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

pub fn read_varint(input: &[u8]) -> anyhow::Result<(u64, &[u8])> {
    let mut bytes = input.iter();
    let mut varint: u64 = 0;
    let mut msb = 1;

    let mut bytes_consumed = 0;
    while msb == 1 {
        let byte = bytes.next().unwrap();
        varint = varint << 7;
        varint += (byte & 0x7F) as u64;
        msb = byte >> 7;
        bytes_consumed += 1;
    }
    Ok((varint, &input[bytes_consumed..]))
}

pub fn get_tables(filepath: &str) -> anyhow::Result<Vec<SqliteSchema>> {
    // Assume no overflow
    let mut file = File::open(filepath)?;
    let mut header = [0; 100];
    file.read_exact(&mut header)?;

    let page_size = u16::from_be_bytes([header[16], header[17]]);
    let mut page1 = Vec::new();
    file.seek(SeekFrom::Start(0))?;
    file.take(page_size.into()).read_to_end(&mut page1)?;

    let page1_header = &page1[100..108];
    let num_tables = u16::from_be_bytes([page1_header[3], page1_header[4]]);
    let _cell_content_offset = u16::from_be_bytes([page1_header[5], page1_header[6]]);
    let cell_pointer_array: Vec<u16> = page1[108..(108 + num_tables * 2) as usize]
        .chunks(2)
        .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
        .collect();

    let mut tables: Vec<SqliteSchema> = Vec::new();

    for i in 0..num_tables {
        let cell_content = &page1[cell_pointer_array[i as usize] as usize..];
        let (payload_size, cell_content) = read_varint(cell_content)?;
        let (_row_id, cell_content) = read_varint(cell_content)?;
        let (payload, _rest) = cell_content.split_at(payload_size as usize);

        let schema = SqliteSchema::from_bytes(payload)?;
        tables.push(schema);
    }
    Ok(tables)
}

pub fn count_table_rows(table_name: &str, filepath: &str) -> anyhow::Result<u64> {
    let tables = get_tables(filepath)?;
    let mut table_map: HashMap<String, SqliteSchema> = HashMap::new();
    for table in tables {
        table_map.insert(table.tbl_name.to_owned(), table);
    }
    if let Some(table_schema) = table_map.get(table_name) {
        let mut file = File::open(filepath)?;
        let mut db_header = [0; 100];
        file.read_exact(&mut db_header)?;
        let page_size = u16::from_be_bytes([db_header[16], db_header[17]]);

        let mut page = Vec::new();
        file.seek(SeekFrom::Start(table_schema.rootpage * page_size as u64))?;
        file.take(page_size.into()).read_to_end(&mut page)?;

        let page_header = &page[..8];
        let num_page_cells = u16::from_be_bytes([page_header[3], page_header[4]]);

        Ok(num_page_cells.into())
    } else {
        Err(anyhow::anyhow!("Table {} not found.", table_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_from_bytes() {
        let x: [u8; 3] = [0xAC, 0x02, 0xAA]; // last byte should be ignored
        let varint = read_varint(&x).unwrap().0;

        assert_eq!(varint, 5634);
    }
}

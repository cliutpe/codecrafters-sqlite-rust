use crate::record::{parse_records, RecordField};
use crate::sql_parser::{parse_create_table, parse_first_word};
use crate::sqlite_schema::SqliteSchema;
use anyhow::{self, bail};
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

pub fn get_page_size(filepath: &str) -> anyhow::Result<u16> {
    let mut file = File::open(filepath)?;
    let mut db_header = [0; 100];
    file.read_exact(&mut db_header)?;
    let page_size = u16::from_be_bytes([db_header[16], db_header[17]]);

    Ok(page_size)
}

pub fn read_page(filepath: &str, page_size: u16, page_num: u64) -> anyhow::Result<Vec<u8>> {
    let mut file = File::open(filepath)?;
    let mut page = Vec::new();
    file.seek(SeekFrom::Start((page_num - 1) * page_size as u64))?;
    file.take(page_size.into()).read_to_end(&mut page)?;
    Ok(page)
}

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
    let page_size = get_page_size(filepath)?;
    let page1 = read_page(filepath, page_size, 1)?;
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

pub fn get_table_name_to_schema_map(
    filepath: &str,
) -> anyhow::Result<HashMap<String, SqliteSchema>> {
    let page_size = get_page_size(filepath)?;
    let page1 = read_page(filepath, page_size, 1)?;

    let page1_header = &page1[100..108];
    let num_tables = u16::from_be_bytes([page1_header[3], page1_header[4]]);
    let _cell_content_offset = u16::from_be_bytes([page1_header[5], page1_header[6]]);
    let cell_pointer_array: Vec<u16> = page1[108..(108 + num_tables * 2) as usize]
        .chunks(2)
        .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
        .collect();

    let mut tables: HashMap<String, SqliteSchema> = HashMap::new();

    for i in 0..num_tables {
        let cell_content = &page1[cell_pointer_array[i as usize] as usize..];
        let (payload_size, cell_content) = read_varint(cell_content)?;
        let (_row_id, cell_content) = read_varint(cell_content)?;
        let (payload, _rest) = cell_content.split_at(payload_size as usize);

        let schema = SqliteSchema::from_bytes(payload)?;
        tables.insert(schema.tbl_name.to_owned(), schema);
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
        let page_size = get_page_size(filepath)?;
        let page = read_page(filepath, page_size, table_schema.rootpage)?;

        let page_header = &page[..8];
        let num_page_cells = u16::from_be_bytes([page_header[3], page_header[4]]);

        Ok(num_page_cells.into())
    } else {
        Err(anyhow::anyhow!("Table {} not found.", table_name))
    }
}

pub fn get_records_from_table(
    table_name: &str,
    field: &str,
    filepath: &str,
) -> anyhow::Result<Vec<RecordField>> {
    let table_map = get_table_name_to_schema_map(filepath)?;

    if let Some(table_schema) = table_map.get(table_name) {
        let page_size = get_page_size(filepath)?;

        let create_statement = table_schema.sql.clone();
        println!("{:?}", table_schema.sql);
        let field_names = parse_create_table(create_statement.as_str())
            // FIXME: the map_err with a closure that never fails is here so that the IResult, which contains the create statement won't propagate
            // It never fails so I can just unwrap
            .map_err(|_| parse_create_table(" "))
            .unwrap()
            .1
            .iter()
            .map(|s| parse_first_word(s).unwrap().1)
            .collect::<Vec<&str>>();
        let mut field_name_map = HashMap::new();
        for (i, field_name) in field_names.iter().enumerate() {
            field_name_map.insert(*field_name, i);
        }
        if let Some(field_index) = field_name_map.get(field) {
            let page = read_page(filepath, page_size, table_schema.rootpage)?;

            let page_header = &page[..8];
            let num_page_cells = u16::from_be_bytes([page_header[3], page_header[4]]);
            let cell_pointer_array: Vec<u16> = page[8..(100 + num_page_cells * 2) as usize]
                .chunks(2)
                .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
                .collect();

            let mut records = Vec::new();
            for i in 0..num_page_cells {
                let cell_content = &page[cell_pointer_array[i as usize] as usize..];
                let (payload_size, cell_content) = read_varint(cell_content)?;
                let (_row_id, cell_content) = read_varint(cell_content)?;
                let (payload, _rest) = cell_content.split_at(payload_size as usize);
                let record = parse_records(payload)?;
                records.push(record[*field_index].clone());
            }
            Ok(records)
        } else {
            bail!("Field {} not found in table", field)
        }
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

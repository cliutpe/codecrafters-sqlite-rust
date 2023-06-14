pub mod sqlite_schema;
pub mod util;
use anyhow::{bail, Result};

use crate::util::read_varint;
use sqlite_schema::SqliteSchema;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            #[allow(unused_variables)]
            let page_size = u16::from_be_bytes([header[16], header[17]]);

            // Assume page1 is a b-tree leaf page: 0x0d
            let mut page1_header = [0; 8];
            file.read_exact(&mut page1_header)?;
            #[allow(unused_variables)]
            let num_tables = u16::from_be_bytes([page1_header[3], page1_header[4]]);
            // You can use print statements as follows for debugging, they'll be visible when running tests.
            println!("Logs from your program will appear here!");

            // Uncomment this block to pass the first stage
            println!("database page size: {}", page_size);
            println!("number of tables: {}", num_tables);
        }
        ".tables" => {
            // Assume no overflow
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            let page_size = u16::from_be_bytes([header[16], header[17]]);
            println!("database page size: {}", page_size);
            let mut page1 = Vec::new();
            file.seek(SeekFrom::Start(0))?;
            file.take(page_size.into()).read_to_end(&mut page1)?;

            let page1_header = &page1[100..108];
            let num_tables = u16::from_be_bytes([page1_header[3], page1_header[4]]);
            println!("num tables: {}", num_tables);
            let cell_content_offset = u16::from_be_bytes([page1_header[5], page1_header[6]]);
            println!("cell start at: {}", cell_content_offset);
            let cell_pointer_array: Vec<u16> = page1[108..(108 + num_tables * 2) as usize]
                .chunks(2)
                .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
                .collect();
            println!("cell pointer array: {:?}", cell_pointer_array);

            let mut table_names: Vec<String> = Vec::new();

            for i in 0..num_tables {
                let cell_content = &page1[cell_pointer_array[i as usize] as usize..];
                let (payload_size, cell_content) = read_varint(cell_content)?;
                println!("payload size (including overflow): {}", payload_size);
                let (_row_id, cell_content) = read_varint(cell_content)?;
                println!("row id: {}", _row_id);
                println!("length left: {}", cell_content.len());
                let (payload, _rest) = cell_content.split_at(payload_size as usize);

                let schema = SqliteSchema::from_bytes(payload)?;
                table_names.push(schema.tbl_name.clone());
                println!("{:?}", schema.tbl_name)
            }
            println!("{}", table_names.join(" "));
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

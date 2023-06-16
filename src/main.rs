pub mod sqlite_schema;
pub mod util;
use anyhow::{bail, Result};

use crate::util::read_varint;
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2].as_str().split(" ").collect::<Vec<&str>>();

    match command[0] {
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
            let tables = util::get_tables(&args[1])?;

            let mut table_names: Vec<String> = Vec::new();
            for (table_name, _table_schema) in tables.into_iter() {
                table_names.push(table_name);
            }
            println!("{}", table_names.join(" "));
        }
        "SELECT" => {
            assert_eq!(command[1], "COUNT(*)");
            assert_eq!(command[2], "FROM");
            let table_name = command[3];
            let tables = util::get_tables(&args[1])?;
        }
        _ => bail!("Missing or invalid command passed: {:?}", command),
    }

    Ok(())
}

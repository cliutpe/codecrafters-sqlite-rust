pub mod record;
pub mod sql_parser;
pub mod sqlite_schema;
pub mod util;
use anyhow::{bail, Result};

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

    match command[0].to_lowercase().as_str() {
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
            for table in tables {
                table_names.push(table.tbl_name);
            }
            println!("{}", table_names.join(" "));
        }
        "select" => match command[1].to_lowercase().as_str() {
            "count(*)" => {
                assert_eq!(command[2].to_lowercase().as_str(), "from");
                let table_name = command[3];
                let num_rows = util::count_table_rows(table_name, &args[1])?;
                println!("{}", num_rows);
            }
            _ => {
                let select_statement = sql_parser::parse_select_statement(&args[2].to_lowercase())
                    .unwrap()
                    .1; //FIXME: lifetime...
                let fields = select_statement
                    .selector
                    .split(',')
                    .map(|s| s.trim())
                    .collect::<Vec<&str>>();

                let table_name = select_statement.from.as_str();
                let records = util::get_records_from_table(table_name, fields, &args[1])?;

                for record in records {
                    let record_strings = record
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>();
                    println!("{}", record_strings.join("|"));
                }
            }
        },
        _ => bail!("Missing or invalid command passed: {:?}", command),
    }

    Ok(())
}

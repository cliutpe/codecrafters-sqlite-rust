use anyhow::{bail, Result};
use nom::bytes::complete::take_until;
use nom::bytes::complete::{is_not, tag};
use nom::character::complete::multispace0;
use nom::combinator::map;
use nom::multi::separated_list0;
use nom::sequence::{delimited, preceded};
use nom::IResult;

fn parse_list(input: &str) -> IResult<&str, Vec<&str>> {
    let single_field_parser = preceded(multispace0, is_not(",)"));
    separated_list0(tag(","), single_field_parser)(input)
}

pub fn parse_create_table(input: &str) -> IResult<&str, Vec<&str>> {
    preceded(take_until("("), delimited(tag("("), parse_list, tag(")")))(input)
}

pub fn parse_first_word(input: &str) -> IResult<&str, &str> {
    preceded(multispace0, is_not("\t\n\r "))(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_field_from_create_table() -> Result<()> {
        let statement = "CREATE TABLE student\n(\n\tid integer primary key autoincrement,\n\tname text,\n\tclass text\n)";

        let parsed = parse_create_table(statement)?
            .1
            .iter()
            .map(|s| parse_first_word(s).unwrap().1)
            .collect::<Vec<&str>>();
        println!("{:?}", parsed);
        Ok(())
    }
}

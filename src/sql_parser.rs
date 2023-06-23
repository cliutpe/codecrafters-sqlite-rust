use nom::bytes::complete::take_until;
use nom::bytes::complete::{is_not, tag};
use nom::character::complete::multispace0;
use nom::combinator::map;
use nom::multi::separated_list0;
use nom::sequence::{delimited, preceded, tuple};
use nom::IResult;

#[derive(Debug)]
pub struct SelectStatement {
    pub selector: String, // TODO: is using &str better here?
    pub from: String,
    //conditions: Vec<String>,
}

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

pub fn parse_selector(input: &str) -> IResult<&str, &str> {
    delimited(tag("select"), take_until("from"), multispace0)(input)
}

pub fn parse_from(input: &str) -> IResult<&str, &str> {
    preceded(
        tag("from"),
        delimited(multispace0, is_not("\t\n\r "), multispace0),
    )(input)
}

pub fn parse_select_statement(input: &str) -> IResult<&str, SelectStatement> {
    let parser = preceded(multispace0, tuple((parse_selector, parse_from)));
    // TODO: maybe map_res here? whats the difference?
    map(parser, |(selector, from)| SelectStatement {
        selector: selector.trim().to_owned(),
        from: from.to_owned(),
    })(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
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

    #[test]
    fn test_parse_select_statement() -> Result<()> {
        let statement = "select id, name from student where id > 500";

        let parsed = parse_select_statement(statement)?;
        println!("{:?}", parsed);
        Ok(())
    }
}

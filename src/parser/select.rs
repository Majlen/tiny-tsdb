use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alpha1, alphanumeric0, multispace0, multispace1};
use nom::combinator::{opt, recognize};
use nom::multi::many1;
use nom::sequence::{pair, preceded, terminated, tuple};
use nom::IResult;

#[derive(Debug, PartialEq)]
pub struct Select {
    pub table: String,
    pub fields: Vec<String>,
}

pub fn select_parser(input: &str) -> IResult<&str, Select> {
    let (input, (_, _, fields, table)) = tuple((
        tag_no_case("select"),
        multispace1,
        field_parser,
        table_parser,
    ))(input)?;
    let mut outfields: Vec<String> = Vec::with_capacity(fields.len());
    for field in fields {
        outfields.push(field.to_owned());
    }
    Ok((
        input,
        Select {
            table: table.to_owned(),
            fields: outfields,
        },
    ))
}

fn field_parser(input: &str) -> IResult<&str, Vec<&str>> {
    let (unparsed, fields) = many1(terminated(
        alt((tag("*"), recognize(pair(alpha1, alphanumeric0)))),
        opt(tuple((multispace0, tag(","), multispace0))),
    ))(input)?;
    Ok((unparsed, fields))
}

fn table_parser(input: &str) -> IResult<&str, &str> {
    let (unparsed, table) = preceded(
        tuple((multispace1, tag_no_case("from"), multispace1)),
        recognize(pair(alpha1, alphanumeric0)),
    )(input)?;
    Ok((unparsed, table))
}

#[test]
fn test_basic() {
    assert_eq!(
        select_parser("select x from y"),
        Ok((
            "",
            Select {
                table: "y".to_owned(),
                fields: vec!("x".to_owned())
            }
        ))
    );
    assert_eq!(
        select_parser("select x,y,z from a"),
        Ok((
            "",
            Select {
                table: "a".to_owned(),
                fields: vec!("x".to_owned(), "y".to_owned(), "z".to_owned())
            }
        ))
    );
}

#[test]
fn test_fields() {
    assert_eq!(field_parser("xxx, yyy"), Ok(("", vec!("xxx", "yyy"))));
    assert_eq!(field_parser("aaa,bbb"), Ok(("", vec!("aaa", "bbb"))));
    assert_eq!(
        field_parser("a1a, b0b,c2c"),
        Ok(("", vec!("a1a", "b0b", "c2c")))
    );
}

#[test]
fn test_table() {
    assert_eq!(table_parser(" from table1"), Ok(("", "table1")));
    assert_eq!(table_parser(" FROM table2"), Ok(("", "table2")));
    assert_eq!(
        table_parser(" from table1 where XX"),
        Ok((" where XX", "table1"))
    );
}

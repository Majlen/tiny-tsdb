use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alpha1, alphanumeric0, multispace0, multispace1};
use nom::combinator::{opt, recognize};
use nom::multi::many1;
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
use nom::IResult;
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub struct Insert {
    pub table: String,
    pub values: HashMap<String, String>,
}

pub fn insert_parser(input: &str) -> IResult<&str, Insert> {
    let (input, (_, table, _, fields, _, _, _, values)) = tuple((
        tag_no_case("insert"),
        table_parser,
        multispace1,
        field_parser,
        multispace1,
        tag_no_case("values"),
        multispace1,
        field_parser,
    ))(input)?;
    if fields.len() != values.len() {
        return Err(nom::Err::Error(nom::error::ParseError::from_error_kind(input, nom::error::ErrorKind::LengthValue)))
    }
    let mut outvalues: HashMap<String, String> = HashMap::with_capacity(fields.len());
    for iter in fields.iter().zip(values.iter()) {
        let (field, value) = iter;
        outvalues.insert((*field).to_owned(), (*value).to_owned());
    }
    Ok((
        input,
        Insert {
            table: table.to_owned(),
            values: outvalues,
        },
    ))
}

fn field_parser(input: &str) -> IResult<&str, Vec<&str>> {
    let (unparsed, fields) = delimited(
        tag("("),
        many1(terminated(
            recognize(pair(alpha1, alphanumeric0)),
            opt(tuple((multispace0, tag(","), multispace0))),
        )),
        tag(")"),
    )(input)?;
    Ok((unparsed, fields))
}

fn table_parser(input: &str) -> IResult<&str, &str> {
    let (unparsed, table) = preceded(
        tuple((multispace1, tag_no_case("into"), multispace1)),
        recognize(pair(alpha1, alphanumeric0)),
    )(input)?;

    Ok((unparsed, table))
}

#[test]
fn test_basic() {
    assert_eq!(
        insert_parser("insert into x (a,b) values (c,d)"),
        Ok((
            "",
            Insert {
                table: "x".to_owned(),
                values: HashMap::from([
                    ("a".to_owned(), "c".to_owned()),
                    ("b".to_owned(), "d".to_owned())
                ])
            }
        ))
    );
    assert_eq!(
        insert_parser("insert into x (a,b) values (c)"),
        Err(
            nom::Err::Error(
                nom::error::ParseError::from_error_kind("", nom::error::ErrorKind::LengthValue)
            )
        )
    );
}

#[test]
fn test_fields() {
    assert_eq!(field_parser("(xxx, yyy)"), Ok(("", vec!("xxx", "yyy"))));
    assert_eq!(field_parser("(aaa,bbb)"), Ok(("", vec!("aaa", "bbb"))));
    assert_eq!(
        field_parser("(a1a, b0b,c2c)"),
        Ok(("", vec!("a1a", "b0b", "c2c")))
    );
    assert_eq!(field_parser("(a1a)"), Ok(("", vec!("a1a"))));
}

#[test]
fn test_table() {
    assert_eq!(table_parser(" into table1"), Ok(("", "table1")));
    assert_eq!(table_parser(" INTO table2"), Ok(("", "table2")));
    assert_eq!(
        table_parser(" into table1 where XX"),
        Ok((" where XX", "table1"))
    );
}

use insert::{insert_parser, Insert};
use nom::branch::alt;
use nom::combinator::map;
use nom::IResult;
use select::{select_parser, Select};

pub mod insert;
pub mod select;

#[derive(Debug, PartialEq)]
pub enum SqlStatement {
    Select(Select),
    Insert(Insert),
}

pub fn parse(input: &str) -> IResult<&str, SqlStatement> {
    let (input, sql) = alt((
        map(select_parser, |select| SqlStatement::Select(select)),
        map(insert_parser, |insert| SqlStatement::Insert(insert)),
    ))(input)?;
    Ok((input, sql))
}

#[test]
fn test_basic() {
    use std::collections::HashMap;
    assert_eq!(
        parse("select x from y"),
        Ok((
            "",
            SqlStatement::Select(Select {
                table: "y".to_owned(),
                fields: vec!("x".to_owned()),
                conditions: vec![],
            })
        ))
    );
    assert_eq!(
        parse("insert into x (y,z) values (a,b)"),
        Ok(("", SqlStatement::Insert(Insert {
            table: "x".to_owned(),
            values: HashMap::from([
                ("y".to_owned(), "a".to_owned()),
                ("z".to_owned(), "b".to_owned())
            ])
        })))
    );
    assert_eq!(
        parse("bla bla"),
        Err(nom::Err::Error(nom::error::Error {
            input: "bla bla",
            code: nom::error::ErrorKind::Tag
        }))
    );
}

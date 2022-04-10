use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alpha1, alphanumeric0, digit1, multispace0, multispace1};
use nom::combinator::{opt, recognize};
use nom::multi::many1;
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
use nom::IResult;

#[derive(Debug, PartialEq)]
pub struct Select {
    pub table: String,
    pub fields: Vec<String>,
    pub conditions: Vec<Condition>,
}

#[derive(Debug, PartialEq)]
pub enum Operator {
    Eq,
    Ge,
    Gt,
    Le,
    Lt,
    Ne,
}

#[derive(Debug, PartialEq)]
pub struct Condition {
    pub field: String,
    pub operator: Operator,
    pub value: String,
}

pub fn select_parser(input: &str) -> IResult<&str, Select> {
    let (input, (_, _, fields, table, conditions)) = tuple((
        tag_no_case("select"),
        multispace1,
        field_parser,
        table_parser,
        opt(preceded(multispace1, where_parser)),
    ))(input)?;

    let mut outfields: Vec<String> = Vec::with_capacity(fields.len());
    for field in fields {
        outfields.push(field.to_owned());
    }
    let conds = match conditions {
        Some(vec) => vec,
        None => vec![],
    };

    Ok((
        input,
        Select {
            table: table.to_owned(),
            fields: outfields,
            conditions: conds,
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

// for now only AND is supported
fn where_parser(input: &str) -> IResult<&str, Vec<Condition>> {
    let (unparsed, (_, _, conditions)) = tuple((
        tag_no_case("where"),
        multispace1,
        many1(terminated(
            condition_parser,
            opt(delimited(multispace1, tag_no_case("and"), multispace1)),
        )),
    ))(input)?;

    Ok((unparsed, conditions))
}

fn condition_parser(input: &str) -> IResult<&str, Condition> {
    let (unparsed, (field, _, operator, _, value)) = tuple((
        recognize(pair(alpha1, alphanumeric0)),
        multispace0,
        alt((
            tag("="),
            tag(">="),
            tag("<="),
            tag("!="),
            tag(">"),
            tag("<"),
        )),
        multispace0,
        alt((
            delimited(tag("'"), recognize(pair(alpha1, alphanumeric0)), tag("'")),
            digit1,
        )),
    ))(input)?;

    let out_operator = match operator {
        "=" => Operator::Eq,
        ">=" => Operator::Ge,
        ">" => Operator::Gt,
        "<=" => Operator::Le,
        "<" => Operator::Lt,
        "!=" => Operator::Ne,
        &_ => Operator::Eq, // This cannot happen, as it wouldn't match above
    };

    let cond = Condition {
        field: field.to_owned(),
        operator: out_operator,
        value: value.to_owned(),
    };

    Ok((unparsed, cond))
}

#[test]
fn test_basic() {
    assert_eq!(
        select_parser("select x from y"),
        Ok((
            "",
            Select {
                table: "y".to_owned(),
                fields: vec!("x".to_owned()),
                conditions: vec![]
            }
        ))
    );
    assert_eq!(
        select_parser("select x,y,z from a"),
        Ok((
            "",
            Select {
                table: "a".to_owned(),
                fields: vec!("x".to_owned(), "y".to_owned(), "z".to_owned()),
                conditions: vec![]
            }
        ))
    );
}

#[test]
fn test_with_where() {
    assert_eq!(
        select_parser("select x from y where x = 'y'"),
        Ok((
            "",
            Select {
                table: "y".to_owned(),
                fields: vec!["x".to_owned()],
                conditions: vec![Condition {
                    field: "x".to_owned(),
                    value: "y".to_owned(),
                    operator: Operator::Eq
                }]
            }
        ))
    );
    assert_eq!(
        select_parser("select x from y where x = 10"),
        Ok((
            "",
            Select {
                table: "y".to_owned(),
                fields: vec!["x".to_owned()],
                conditions: vec![Condition {
                    field: "x".to_owned(),
                    value: "10".to_owned(),
                    operator: Operator::Eq
                }]
            }
        ))
    );
    assert_eq!(
        select_parser("select x from y where x > 10 AND x <= 20"),
        Ok((
            "",
            Select {
                table: "y".to_owned(),
                fields: vec!["x".to_owned()],
                conditions: vec![
                    Condition {
                        field: "x".to_owned(),
                        value: "10".to_owned(),
                        operator: Operator::Gt
                    },
                    Condition {
                        field: "x".to_owned(),
                        value: "20".to_owned(),
                        operator: Operator::Le
                    },
                ]
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

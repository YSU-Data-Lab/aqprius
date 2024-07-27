use nom::{
    branch::alt,
    bytes::complete::{tag, take_until},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt},
    multi::separated_list0,
    sequence::{delimited, preceded, terminated},
    IResult,
};
#[allow(unused_imports)]
use nom::{bytes::complete::take_till, multi::separated_list1};

#[derive(Debug, Clone)]
pub struct SQLQuery {
    pub select: Select,
}

impl SQLQuery {
    fn new(select: Select) -> Self {
        SQLQuery { select }
    }

    pub fn get_select(&self) -> &Select {
        &self.select
    }
}

#[derive(Debug, Clone)]
pub struct Select {
    function: String,
    table: Vec<String>,
    where_clause: Option<Vec<Where>>,
}

impl Select {
    fn new(function: String, table: Vec<String>, where_clause: Option<Vec<Where>>) -> Self {
        Select {
            function,
            table,
            where_clause,
        }
    }
    //getter methods for the Select struct
    pub fn get_function(&self) -> &str {
        &self.function
    }

    pub fn get_table(&self) -> &Vec<String> {
        &self.table
    }

    pub fn get_where_clause(&self) -> &Option<Vec<Where>> {
        &self.where_clause
    }
}

#[derive(Debug, Clone)]
pub struct Where {
    left: String,
    right: String,
    operator: String,
}

impl Where {
    pub fn new(left: String, right: String, operator: String) -> Where {
        Where {
            left,
            right,
            operator,
        }
    }
    //getter methods for the Where struct
    pub fn get_left(&self) -> &str {
        &self.left
    }

    pub fn get_right(&self) -> &str {
        &self.right
    }

    pub fn get_operator(&self) -> &str {
        &self.operator
    }
}

pub fn parse_sql_query(input: &str) -> IResult<&str, SQLQuery> {
    let (input, select) = parse_select(input)?;
    Ok((input, SQLQuery::new(select)))
}

pub fn parse_select(input: &str) -> IResult<&str, Select> {
    let (input, _) = multispace0(input)?;
    let (input, function) = parse_function(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("from")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table) = parse_table_list(input)?;
    let (input, where_clause) = opt(parse_where_clause)(input)?;

    Ok((input, Select::new(function, table, where_clause)))
}

fn parse_function(input: &str) -> IResult<&str, String> {
    let (input, _) = tag("select")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, function) = alt((
        map(
            preceded(
                tag("count"),
                delimited(multispace0, tag("(*)"), multispace0),
            ),
            |inside_parens| format!("count{}", inside_parens),
        ),
        // add other functions here
    ))(input)?;

    Ok((input, function.to_string()))
}

fn parse_table_list(input: &str) -> IResult<&str, Vec<String>> {
    let (input, table_list) = separated_list1(
        delimited(multispace0, char(','), multispace0),
        map(
            terminated(take_until("where"), opt(char(','))),
            |s: &str| s.trim().to_string(),
        ),
    )(input)?;

    Ok((input, table_list))
}

pub fn parse_where_clause(input: &str) -> IResult<&str, Vec<Where>> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("where")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, where_list) = separated_list0(
        delimited(multispace0, tag("and"), multispace0),
        parse_where_condition,
    )(input)?;

    Ok((input, where_list))
}

pub fn parse_where_condition(input: &str) -> IResult<&str, Where> {
    let (input, left) = take_till(|c: char| c == '=' || c == '<' || c == '>')(input)?;

    let (input, operator) = alt((tag("="), tag("<"), tag(">")))(input)?;
    let operator = operator.to_string();

    let (input, _) = multispace1(input)?;
    let (input, right) = alt((map(take_till(|c: char| c.is_whitespace()), |s: &str| {
        s.trim().to_string()
    }),))(input)?;

    let left = left.trim().to_string();

    Ok((input, Where::new(left, right, operator)))
}
#[allow(dead_code)]
//get the value of where clause
pub fn get_join_conditions(conditions: &[Where]) -> Vec<String> {
    conditions
        .iter()
        .map(|condition| {
            format!(
                "{} {} {}",
                condition.get_left(),
                condition.get_operator(),
                condition.get_right()
            )
        })
        .collect()
}

use crate::parser::Where;
use rusqlite::{Connection, Result, Row};
use std::collections::HashMap;
use std::time::Instant;

macro_rules! insert_to_hashmap {
    ($hashmap:expr, $sample:expr, $($field:ident),+) => {
        $(
            $hashmap.insert(stringify!($field).to_string(), $sample.$field.to_string());
        )+
    };
}

//S2_sample data
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct S2Sample {
    //LineItem fields
    pub l_orderkey: i32,
    pub l_partkey: i32,
    pub l_suppkey: i32,
    pub l_linenumber: i32,
    pub l_quantity: f64,
    pub l_extendedprice: f64,
    pub l_discount: f64,
    pub l_tax: f64,
    pub l_returnflag: String,
    pub l_linestatus: String,
    pub l_shipdate: String,
    pub l_commitdate: String,
    pub l_receiptdate: String,
    pub l_shipinstruct: String,
    pub l_shipmode: String,
    pub l_comment: String,

    //Orders fields
    pub o_orderkey: i32,
    pub o_custkey: i32,
    pub o_orderstatus: String,
    pub o_totalprice: f64,
    pub o_orderdate: String,
    pub o_orderpriority: String,
    pub o_clerk: String,
    pub o_shippriority: i32,
    pub o_comment: String,
}

impl S2Sample {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(S2Sample {
            l_orderkey: row.get(0)?,
            l_partkey: row.get(1)?,
            l_suppkey: row.get(2)?,
            l_linenumber: row.get(3)?,
            l_quantity: row.get(4)?,
            l_extendedprice: row.get(5)?,
            l_discount: row.get(6)?,
            l_tax: row.get(7)?,
            l_returnflag: row.get(8)?,
            l_linestatus: row.get(9)?,
            l_shipdate: row.get(10)?,
            l_commitdate: row.get(11)?,
            l_receiptdate: row.get(12)?,
            l_shipinstruct: row.get(13)?,
            l_shipmode: row.get(14)?,
            l_comment: row.get(15)?,
            o_orderkey: row.get(16)?,
            o_custkey: row.get(17)?,
            o_orderstatus: row.get(18)?,
            o_totalprice: row.get(19)?,
            o_orderdate: row.get(20)?,
            o_orderpriority: row.get(21)?,
            o_clerk: row.get(22)?,
            o_shippriority: row.get(23)?,
            o_comment: row.get(24)?,
        })
    }
}

//fetch the data from database
pub fn fetch_s2_sample(conn: &Connection) -> Result<Vec<S2Sample>> {
    let mut stmt = conn.prepare("SELECT * FROM s2_sample")?;
    let s2_samples_iter = stmt.query_map([], S2Sample::from_row)?;

    let mut s2_samples = Vec::new();
    for sample in s2_samples_iter {
        s2_samples.push(sample?);
    }

    Ok(s2_samples)
}

//convert struct into hashmap for easier search
pub fn s2_sample_to_hashmap(samples: &[S2Sample]) -> Vec<HashMap<String, String>> {
    let start_time = Instant::now();

    let hashmaps = samples
        .iter()
        .map(|sample| {
            let mut hashmap = HashMap::new();
            insert_to_hashmap!(
                hashmap,
                sample,
                o_orderkey,
                l_partkey,
                l_suppkey,
                l_linenumber,
                l_quantity,
                l_extendedprice,
                l_discount,
                l_tax,
                l_returnflag,
                l_linestatus,
                l_shipdate,
                l_commitdate,
                l_receiptdate,
                l_shipinstruct,
                l_shipmode,
                l_comment,
                o_custkey,
                o_orderstatus,
                o_totalprice,
                o_orderdate,
                o_orderpriority,
                o_clerk,
                o_shippriority,
                o_comment
            );
            hashmap
        })
        .collect();

    let end_time = Instant::now();
    let _execution_time = end_time - start_time;

    // println!(
    //     "Execution time s2_sample_to_hashmap: {:.3}",
    //     execution_time.as_secs_f64()
    // );

    hashmaps
}

//S3_sample table data
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct S3Sample {
    //LineItem fields
    pub l_orderkey: i32,
    pub l_partkey: i32,
    pub l_suppkey: i32,
    pub l_linenumber: i32,
    pub l_quantity: f64,
    pub l_extendedprice: f64,
    pub l_discount: f64,
    pub l_tax: f64,
    pub l_returnflag: String,
    pub l_linestatus: String,
    pub l_shipdate: String,
    pub l_commitdate: String,
    pub l_receiptdate: String,
    pub l_shipinstruct: String,
    pub l_shipmode: String,
    pub l_comment: String,

    //Orders fields
    pub o_orderkey: i32,
    pub o_orderstatus: String,
    pub o_totalprice: f64,
    pub o_orderdate: String,
    pub o_orderpriority: String,
    pub o_clerk: String,
    pub o_shippriority: i32,
    pub o_comment: String,

    //Customer fields
    pub c_custkey: i32,
    pub c_name: String,
    pub c_address: String,
    pub c_nationkey: i32,
    pub c_phone: String,
    pub c_acctbal: f64,
    pub c_mktsegment: String,
    pub c_comment: String,
}

impl S3Sample {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(S3Sample {
            l_orderkey: row.get(0)?,
            l_partkey: row.get(1)?,
            l_suppkey: row.get(2)?,
            l_linenumber: row.get(3)?,
            l_quantity: row.get(4)?,
            l_extendedprice: row.get(5)?,
            l_discount: row.get(6)?,
            l_tax: row.get(7)?,
            l_returnflag: row.get(8)?,
            l_linestatus: row.get(9)?,
            l_shipdate: row.get(10)?,
            l_commitdate: row.get(11)?,
            l_receiptdate: row.get(12)?,
            l_shipinstruct: row.get(13)?,
            l_shipmode: row.get(14)?,
            l_comment: row.get(15)?,
            o_orderkey: row.get(16)?,
            o_orderstatus: row.get(18)?,
            o_totalprice: row.get(19)?,
            o_orderdate: row.get(20)?,
            o_orderpriority: row.get(21)?,
            o_clerk: row.get(22)?,
            o_shippriority: row.get(23)?,
            o_comment: row.get(24)?,
            c_custkey: row.get(25)?,
            c_name: row.get(26)?,
            c_address: row.get(27)?,
            c_nationkey: row.get(28)?,
            c_phone: row.get(29)?,
            c_acctbal: row.get(30)?,
            c_mktsegment: row.get(31)?,
            c_comment: row.get(32)?,
        })
    }
}

//fetch sample data from database
pub fn fetch_s3_sample(conn: &Connection) -> Result<Vec<S3Sample>> {
    let mut stmt = conn.prepare("SELECT * FROM s3_sample")?;
    let s3_samples_iter = stmt.query_map([], S3Sample::from_row)?;

    let mut s3_samples = Vec::new();
    for sample in s3_samples_iter {
        s3_samples.push(sample?);
    }

    Ok(s3_samples)
}

// //function to create a hashmap of s3sample so that we can filter based on the selection condition
pub fn s3_sample_to_hashmap(samples: &[S3Sample]) -> Vec<HashMap<String, String>> {
    samples
        .iter()
        .map(|sample| {
            let mut hashmap = HashMap::new();
            insert_to_hashmap!(
                hashmap,
                sample,
                c_custkey,
                l_orderkey,
                l_partkey,
                l_suppkey,
                l_linenumber,
                l_quantity,
                l_extendedprice,
                l_discount,
                l_tax,
                l_returnflag,
                l_linestatus,
                l_shipdate,
                l_commitdate,
                l_receiptdate,
                l_shipinstruct,
                l_shipmode,
                l_comment,
                o_orderstatus,
                o_totalprice,
                o_orderdate,
                o_orderpriority,
                o_clerk,
                o_shippriority,
                o_comment,
                c_name,
                c_address,
                c_nationkey,
                c_phone,
                c_acctbal,
                c_mktsegment,
                c_comment
            );
            hashmap
        })
        .collect()
}
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct S4Sample {
    //LineItem fields
    pub l_orderkey: i32,
    pub l_partkey: i32,
    pub l_suppkey: i32,
    pub l_linenumber: i32,
    pub l_quantity: f64,
    pub l_extendedprice: f64,
    pub l_discount: f64,
    pub l_tax: f64,
    pub l_returnflag: String,
    pub l_linestatus: String,
    pub l_shipdate: String,
    pub l_commitdate: String,
    pub l_receiptdate: String,
    pub l_shipinstruct: String,
    pub l_shipmode: String,
    pub l_comment: String,

    //Orders fields
    pub o_orderkey: i32,
    pub o_orderstatus: String,
    pub o_totalprice: f64,
    pub o_orderdate: String,
    pub o_orderpriority: String,
    pub o_clerk: String,
    pub o_shippriority: i32,
    pub o_comment: String,

    //Customer fields
    pub c_custkey: i32,
    pub c_name: String,
    pub c_address: String,
    pub c_nationkey: i32,
    pub c_phone: String,
    pub c_acctbal: f64,
    pub c_mktsegment: String,
    pub c_comment: String,

    //Nation fileds
    pub n_nationkey: i32,
    pub n_name: String,
    pub n_regionkey: i32,
    pub n_comment: String,
}

//get all the s4sample data from the sqlite database
impl S4Sample {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(S4Sample {
            l_orderkey: row.get(0)?,
            l_partkey: row.get(1)?,
            l_suppkey: row.get(2)?,
            l_linenumber: row.get(3)?,
            l_quantity: row.get(4)?,
            l_extendedprice: row.get(5)?,
            l_discount: row.get(6)?,
            l_tax: row.get(7)?,
            l_returnflag: row.get(8)?,
            l_linestatus: row.get(9)?,
            l_shipdate: row.get(10)?,
            l_commitdate: row.get(11)?,
            l_receiptdate: row.get(12)?,
            l_shipinstruct: row.get(13)?,
            l_shipmode: row.get(14)?,
            l_comment: row.get(15)?,
            o_orderkey: row.get(16)?,
            o_orderstatus: row.get(18)?,
            o_totalprice: row.get(19)?,
            o_orderdate: row.get(20)?,
            o_orderpriority: row.get(21)?,
            o_clerk: row.get(22)?,
            o_shippriority: row.get(23)?,
            o_comment: row.get(24)?,
            c_custkey: row.get(25)?,
            c_name: row.get(26)?,
            c_address: row.get(27)?,
            c_nationkey: row.get(28)?,
            c_phone: row.get(29)?,
            c_acctbal: row.get(30)?,
            c_mktsegment: row.get(31)?,
            c_comment: row.get(32)?,
            n_nationkey: row.get(33)?,
            n_name: row.get(34)?,
            n_regionkey: row.get(35)?,
            n_comment: row.get(36)?,
        })
    }
}

//fetch sample data from database
pub fn fetch_s4_sample(conn: &Connection) -> Result<Vec<S4Sample>> {
    let mut stmt = conn.prepare("SELECT * FROM s4_sample")?;
    let s4_samples_iter = stmt.query_map([], S4Sample::from_row)?;

    let mut s4_samples = Vec::new();
    for sample in s4_samples_iter {
        s4_samples.push(sample?);
    }

    Ok(s4_samples)
}

//s4sample to hashmap for faster searching
pub fn s4_sample_to_hashmap(samples: &[S4Sample]) -> Vec<HashMap<String, String>> {
    let start_time = Instant::now();

    let hashmaps = samples
        .iter()
        .map(|sample| {
            let mut hashmap = HashMap::new();
            insert_to_hashmap!(
                hashmap,
                sample,
                l_orderkey,
                l_partkey,
                l_suppkey,
                l_linenumber,
                l_quantity,
                l_extendedprice,
                l_discount,
                l_tax,
                l_returnflag,
                l_linestatus,
                l_shipdate,
                l_commitdate,
                l_receiptdate,
                l_shipinstruct,
                l_shipmode,
                l_comment,
                o_orderkey,
                o_orderstatus,
                o_totalprice,
                o_orderdate,
                o_orderpriority,
                o_clerk,
                o_shippriority,
                o_comment,
                c_custkey,
                c_name,
                c_address,
                c_nationkey,
                c_phone,
                c_acctbal,
                c_mktsegment,
                c_comment,
                n_nationkey,
                n_name,
                n_regionkey,
                n_comment
            );
            hashmap
        })
        .collect();

    let end_time = Instant::now();
    let _execution_time = end_time - start_time;

    // println!(
    //     "Execution time s4_sample_to_hashmap: {:.3}",
    //     execution_time.as_secs_f64()
    // );

    hashmaps
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct S5Sample {
    //LineItem fields
    pub l_orderkey: i32,
    pub l_partkey: i32,
    pub l_suppkey: i32,
    pub l_linenumber: i32,
    pub l_quantity: f64,
    pub l_extendedprice: f64,
    pub l_discount: f64,
    pub l_tax: f64,
    pub l_returnflag: String,
    pub l_linestatus: String,
    pub l_shipdate: String,
    pub l_commitdate: String,
    pub l_receiptdate: String,
    pub l_shipinstruct: String,
    pub l_shipmode: String,
    pub l_comment: String,

    //Orders fields
    pub o_orderkey: i32,
    pub o_orderstatus: String,
    pub o_totalprice: f64,
    pub o_orderdate: String,
    pub o_orderpriority: String,
    pub o_clerk: String,
    pub o_shippriority: i32,
    pub o_comment: String,

    //Customer fields
    pub c_custkey: i32,
    pub c_name: String,
    pub c_address: String,
    pub c_nationkey: i32,
    pub c_phone: String,
    pub c_acctbal: f64,
    pub c_mktsegment: String,
    pub c_comment: String,

    //Nation fileds
    pub n_nationkey: i32,
    pub n_name: String,
    pub n_regionkey: i32,
    pub n_comment: String,
    //Region Fields
    pub r_regionkey: i32,
    pub r_name: String,
    pub r_comment: String,
}

impl S5Sample {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(S5Sample {
            l_orderkey: row.get(0)?,
            l_partkey: row.get(1)?,
            l_suppkey: row.get(2)?,
            l_linenumber: row.get(3)?,
            l_quantity: row.get(4)?,
            l_extendedprice: row.get(5)?,
            l_discount: row.get(6)?,
            l_tax: row.get(7)?,
            l_returnflag: row.get(8)?,
            l_linestatus: row.get(9)?,
            l_shipdate: row.get(10)?,
            l_commitdate: row.get(11)?,
            l_receiptdate: row.get(12)?,
            l_shipinstruct: row.get(13)?,
            l_shipmode: row.get(14)?,
            l_comment: row.get(15)?,
            o_orderkey: row.get(16)?,
            o_orderstatus: row.get(18)?,
            o_totalprice: row.get(19)?,
            o_orderdate: row.get(20)?,
            o_orderpriority: row.get(21)?,
            o_clerk: row.get(22)?,
            o_shippriority: row.get(23)?,
            o_comment: row.get(24)?,
            c_custkey: row.get(25)?,
            c_name: row.get(26)?,
            c_address: row.get(27)?,
            c_nationkey: row.get(28)?,
            c_phone: row.get(29)?,
            c_acctbal: row.get(30)?,
            c_mktsegment: row.get(31)?,
            c_comment: row.get(32)?,
            n_nationkey: row.get(33)?,
            n_name: row.get(34)?,
            n_regionkey: row.get(35)?,
            n_comment: row.get(36)?,
            r_regionkey: row.get(37)?,
            r_name: row.get(38)?,
            r_comment: row.get(39)?,
        })
    }
}

//fetch sample data from database
pub fn fetch_s5_sample(conn: &Connection) -> Result<Vec<S5Sample>> {
    let mut stmt = conn.prepare("SELECT * FROM s5_sample")?;
    let s5_samples_iter = stmt.query_map([], S5Sample::from_row)?;

    let mut s5_samples = Vec::new();
    for sample in s5_samples_iter {
        s5_samples.push(sample?);
    }

    Ok(s5_samples)
}

//s5sample to hashmap for faster searching
pub fn s5_sample_to_hashmap(samples: &[S5Sample]) -> Vec<HashMap<String, String>> {
    let start_time = Instant::now();

    let hashmaps = samples
        .iter()
        .map(|sample| {
            let mut hashmap = HashMap::new();
            insert_to_hashmap!(
                hashmap,
                sample,
                l_orderkey,
                l_partkey,
                l_suppkey,
                l_linenumber,
                l_quantity,
                l_extendedprice,
                l_discount,
                l_tax,
                l_returnflag,
                l_linestatus,
                l_shipdate,
                l_commitdate,
                l_receiptdate,
                l_shipinstruct,
                l_shipmode,
                l_comment,
                o_orderkey,
                o_orderstatus,
                o_totalprice,
                o_orderdate,
                o_orderpriority,
                o_clerk,
                o_shippriority,
                o_comment,
                c_custkey,
                c_name,
                c_address,
                c_nationkey,
                c_phone,
                c_acctbal,
                c_mktsegment,
                c_comment,
                n_nationkey,
                n_name,
                n_regionkey,
                n_comment,
                r_regionkey,
                r_name,
                r_comment
            );
            hashmap
        })
        .collect();

    let end_time = Instant::now();
    let _execution_time = end_time - start_time;

    // println!(
    //     "Execution time s5_sample_to_hashmap: {:.3}",
    //     execution_time.as_secs_f64()
    // );

    hashmaps
}

//fn to check for the where condition and return 1 if true or 0
pub fn get_query_result(data: &Vec<HashMap<String, String>>, conditions: &Vec<Where>) -> Vec<i64> {
    let mut results = Vec::with_capacity(data.len());

    for row in data {
        let mut all_conditions_passed = true;
        for condition in conditions {
            let column_value = match row.get(&condition.get_left().to_lowercase()) {
                Some(value) => value.parse::<f64>().unwrap_or(0.0), // parsing string as f64 so that it works on both int and float data types
                None => continue,
            };
            let condition_value = condition.get_right().parse::<f64>().unwrap_or(0.0);
            //matching the comparator and returning true or false based on the condition values
            let condition_result = match condition.get_operator() {
                "<" => column_value < condition_value,
                ">" => column_value > condition_value,
                _ => false,
            };

            all_conditions_passed &= condition_result;
        }

        //inserting 1 if all conditions are true else inserting 0
        let result = if all_conditions_passed { 1 } else { 0 };
        results.push(result);
    }

    results
}

use rusqlite::{Connection, Result, Row};
use std::collections::HashMap;
use std::error::Error;
// use std::time::Instant;

use crate::samples::{S3Sample, S4Sample};

pub struct Nation {
    pub n_nationkey: i32,
    pub n_name: String,
    pub n_regionkey: i32,
    pub n_comment: String,
}

impl Nation {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(Nation {
            n_nationkey: row.get(0)?,
            n_name: row.get(1)?,
            n_regionkey: row.get(2)?,
            n_comment: row.get(3)?,
        })
    }
}

//function to generate s4sample
pub fn generate_s4_sample(
    conn: &Connection,
    sample: &[S3Sample],
) -> Result<Vec<S4Sample>, Box<dyn Error>> {
    let query = "SELECT * FROM nation;"; // SQL query to retrieve nation data

    // let start_time = Instant::now(); // Start measuring time

    // Execute the query and get a streaming iterator
    let mut stmt = conn.prepare(query)?;
    let stream = stmt.query_map([], Nation::from_row)?;

    // Collect the streamed data into a vector
    let nations = stream.collect::<Result<Vec<Nation>, _>>()?;

    let nation_map: HashMap<i32, &Nation> = nations
        .iter()
        .map(|nation| (nation.n_nationkey, nation))
        .collect();

    let s4_samples = sample
        .iter()
        .filter_map(|s3sample| {
            nation_map
                .get(&s3sample.c_nationkey)
                .map(|nation| S4Sample {
                    // Join key
                    n_nationkey: nation.n_nationkey,
                    c_custkey: s3sample.c_custkey,
                    o_orderkey: s3sample.o_orderkey,

                    // LineItem fields
                    l_partkey: s3sample.l_partkey,
                    l_suppkey: s3sample.l_suppkey,
                    l_linenumber: s3sample.l_linenumber,
                    l_quantity: s3sample.l_quantity,
                    l_extendedprice: s3sample.l_extendedprice,
                    l_discount: s3sample.l_discount,
                    l_tax: s3sample.l_tax,
                    l_returnflag: s3sample.l_returnflag.clone(),
                    l_linestatus: s3sample.l_linestatus.clone(),
                    l_shipdate: s3sample.l_shipdate.clone(),
                    l_commitdate: s3sample.l_commitdate.clone(),
                    l_receiptdate: s3sample.l_receiptdate.clone(),
                    l_shipinstruct: s3sample.l_shipinstruct.clone(),
                    l_shipmode: s3sample.l_shipmode.clone(),
                    l_comment: s3sample.l_comment.clone(),

                    // Orders fields
                    o_orderstatus: s3sample.o_orderstatus.clone(),
                    o_totalprice: s3sample.o_totalprice,
                    o_orderdate: s3sample.o_orderdate.clone(),
                    o_orderpriority: s3sample.o_orderpriority.clone(),
                    o_clerk: s3sample.o_clerk.clone(),
                    o_shippriority: s3sample.o_shippriority,
                    o_comment: s3sample.o_comment.clone(),

                    // Customer fields
                    c_name: s3sample.c_name.clone(),
                    c_address: s3sample.c_address.clone(),
                    c_nationkey: s3sample.c_nationkey,
                    c_phone: s3sample.c_phone.clone(),
                    c_acctbal: s3sample.c_acctbal,
                    c_mktsegment: s3sample.c_mktsegment.clone(),
                    c_comment: s3sample.c_comment.clone(),

                    // Nation fields
                    n_name: nation.n_name.clone(),
                    n_regionkey: nation.n_regionkey,
                    n_comment: nation.n_comment.clone(),
                })
        })
        .collect();

    // let end_time = Instant::now(); // Stop measuring time
    // let execution_time = end_time - start_time;

    // println!(
    //     "Execution time S4Sample: {:.3}",
    //     execution_time.as_secs_f64()
    // );

    Ok(s4_samples)
}

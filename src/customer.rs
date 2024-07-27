use rusqlite::{Connection, Result, Row};
// use std::time::Instant;
use std::{collections::HashMap, error::Error};

use crate::samples::{S2Sample, S3Sample};

#[derive(Debug, Clone)]
pub struct Customer {
    pub c_custkey: i32,
    pub c_name: String,
    pub c_address: String,
    pub c_nationkey: i32,
    pub c_phone: String,
    pub c_acctbal: f64,
    pub c_mktsegment: String,
    pub c_comment: String,
}

impl Customer {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(Customer {
            c_custkey: row.get(0)?,
            c_name: row.get(1)?,
            c_address: row.get(2)?,
            c_nationkey: row.get(3)?,
            c_phone: row.get(4)?,
            c_acctbal: row.get(5)?,
            c_mktsegment: row.get(6)?,
            c_comment: row.get(7)?,
        })
    }
}

pub fn generate_s3_sample(
    conn: &Connection,
    sample: &[S2Sample],
) -> Result<Vec<S3Sample>, Box<dyn Error>> {
    let query = "SELECT * FROM customer;"; // SQL query to retrieve customer data

    // let start_time = Instant::now(); // Start measuring time

    // Execute the query and get a streaming iterator
    let mut stmt = conn.prepare(query)?;
    let stream = stmt.query_map([], Customer::from_row)?;

    // Collect the streamed data into a vector
    let customers = stream.collect::<Result<Vec<Customer>, _>>()?;

    let customer_map: HashMap<i32, &Customer> = customers
        .iter()
        .map(|customer| (customer.c_custkey, customer))
        .collect();

    let s3_samples = sample
        .iter()
        .filter_map(|s2sample| {
            customer_map
                .get(&s2sample.o_custkey)
                .map(|customer| S3Sample {
                    // Join key
                    c_custkey: s2sample.o_custkey,
                    o_orderkey: s2sample.o_orderkey,

                    // LineItem fields
                    l_partkey: s2sample.l_partkey,
                    l_suppkey: s2sample.l_suppkey,
                    l_linenumber: s2sample.l_linenumber,
                    l_quantity: s2sample.l_quantity,
                    l_extendedprice: s2sample.l_extendedprice,
                    l_discount: s2sample.l_discount,
                    l_tax: s2sample.l_tax,
                    l_returnflag: s2sample.l_returnflag.clone(),
                    l_linestatus: s2sample.l_linestatus.clone(),
                    l_shipdate: s2sample.l_shipdate.clone(),
                    l_commitdate: s2sample.l_commitdate.clone(),
                    l_receiptdate: s2sample.l_receiptdate.clone(),
                    l_shipinstruct: s2sample.l_shipinstruct.clone(),
                    l_shipmode: s2sample.l_shipmode.clone(),
                    l_comment: s2sample.l_comment.clone(),

                    // Orders fields
                    o_orderstatus: s2sample.o_orderstatus.clone(),
                    o_totalprice: s2sample.o_totalprice,
                    o_orderdate: s2sample.o_orderdate.clone(),
                    o_orderpriority: s2sample.o_orderpriority.clone(),
                    o_clerk: s2sample.o_clerk.clone(),
                    o_shippriority: s2sample.o_shippriority,
                    o_comment: s2sample.o_comment.clone(),

                    // Customer fields
                    c_name: customer.c_name.clone(),
                    c_address: customer.c_address.clone(),
                    c_nationkey: customer.c_nationkey,
                    c_phone: customer.c_phone.clone(),
                    c_acctbal: customer.c_acctbal,
                    c_mktsegment: customer.c_mktsegment.clone(),
                    c_comment: customer.c_comment.clone(),
                })
        })
        .collect();

    // let end_time = Instant::now(); // Stop measuring time
    // let execution_time = end_time - start_time;

    // println!(
    //     "Execution time S3Sample: {:.3}s",
    //     execution_time.as_secs_f64()
    // );

    Ok(s3_samples)
}

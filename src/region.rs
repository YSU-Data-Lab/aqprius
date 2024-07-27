use rusqlite::{Connection, Result, Row};
// use std::time::Instant;
use std::{collections::HashMap, error::Error};

use crate::samples::{S4Sample, S5Sample};

pub struct Region {
    pub r_regionkey: i32,
    pub r_name: String,
    pub r_comment: String,
}

impl Region {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(Region {
            r_regionkey: row.get(0)?,
            r_name: row.get(1)?,
            r_comment: row.get(2)?,
        })
    }
}

//function to generate s5_sample
pub fn generate_s5_sample(
    conn: &Connection,
    s4samples: &[S4Sample],
) -> Result<Vec<S5Sample>, Box<dyn Error>> {
    let query = "SELECT * FROM region;"; // SQL query to retrieve region data

    // let start_time = Instant::now(); // Start measuring time

    // Execute the query and get a streaming iterator
    let mut stmt = conn.prepare(query)?;
    let stream = stmt.query_map([], Region::from_row)?;

    // Collect the streamed data into a vector
    let regions = stream.collect::<Result<Vec<Region>, _>>()?;

    let region_map: HashMap<i32, &Region> = regions
        .iter()
        .map(|region| (region.r_regionkey, region))
        .collect();

    let s5_samples = s4samples
        .iter()
        .filter_map(|s4sample| {
            region_map
                .get(&s4sample.n_regionkey)
                .map(|region| S5Sample {
                    // Join key
                    r_regionkey: region.r_regionkey,
                    n_nationkey: s4sample.n_nationkey,
                    c_custkey: s4sample.c_custkey,
                    o_orderkey: s4sample.o_orderkey,

                    // LineItem fields
                    l_partkey: s4sample.l_partkey,
                    l_suppkey: s4sample.l_suppkey,
                    l_linenumber: s4sample.l_linenumber,
                    l_quantity: s4sample.l_quantity,
                    l_extendedprice: s4sample.l_extendedprice,
                    l_discount: s4sample.l_discount,
                    l_tax: s4sample.l_tax,
                    l_returnflag: s4sample.l_returnflag.clone(),
                    l_linestatus: s4sample.l_linestatus.clone(),
                    l_shipdate: s4sample.l_shipdate.clone(),
                    l_commitdate: s4sample.l_commitdate.clone(),
                    l_receiptdate: s4sample.l_receiptdate.clone(),
                    l_shipinstruct: s4sample.l_shipinstruct.clone(),
                    l_shipmode: s4sample.l_shipmode.clone(),
                    l_comment: s4sample.l_comment.clone(),

                    // Orders fields
                    o_orderstatus: s4sample.o_orderstatus.clone(),
                    o_totalprice: s4sample.o_totalprice,
                    o_orderdate: s4sample.o_orderdate.clone(),
                    o_orderpriority: s4sample.o_orderpriority.clone(),
                    o_clerk: s4sample.o_clerk.clone(),
                    o_shippriority: s4sample.o_shippriority,
                    o_comment: s4sample.o_comment.clone(),

                    // Customer fields
                    c_name: s4sample.c_name.clone(),
                    c_address: s4sample.c_address.clone(),
                    c_nationkey: s4sample.c_nationkey,
                    c_phone: s4sample.c_phone.clone(),
                    c_acctbal: s4sample.c_acctbal,
                    c_mktsegment: s4sample.c_mktsegment.clone(),
                    c_comment: s4sample.c_comment.clone(),

                    // Nation fields
                    n_name: s4sample.n_name.clone(),
                    n_regionkey: s4sample.n_regionkey,
                    n_comment: s4sample.n_comment.clone(),

                    // Region fields
                    r_name: region.r_name.clone(),
                    r_comment: region.r_comment.clone(),
                })
        })
        .collect();

    // let end_time = Instant::now(); // Stop measuring time
    // let execution_time = end_time - start_time;

    // println!(
    //     "Execution time S5Sample: {:.3}",
    //     execution_time.as_secs_f64()
    // );

    Ok(s5_samples)
}

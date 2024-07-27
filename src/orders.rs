use crate::{data_sampling::S1Sample, samples::S2Sample};
use rusqlite::{Connection, Result, Row};
// use std::time::Instant;
// use dashmap::DashMap;
// use rayon::prelude::*;
use std::{collections::HashMap, error::Error};
#[derive(Debug, Clone)]
pub struct Orders {
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

impl Orders {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(Orders {
            o_orderkey: row.get(0)?,
            o_custkey: row.get(1)?,
            o_orderstatus: row.get(2)?,
            o_totalprice: row.get(3)?,
            o_orderdate: row.get(4)?,
            o_orderpriority: row.get(5)?,
            o_clerk: row.get(6)?,
            o_shippriority: row.get(7)?,
            o_comment: row.get(8)?,
        })
    }
}

pub fn generate_s2_sample(
    conn: &Connection,
    sample: &[S1Sample],
) -> Result<Vec<S2Sample>, Box<dyn Error>> {
    // Define the SQL query to retrieve all rows from the orders table
    let query = "SELECT * FROM orders;";

    // let start_time = Instant::now(); // Start measuring time

    // Execute the query and get a streaming iterator
    let mut stmt = conn.prepare(query)?;
    let stream = stmt.query_map([], Orders::from_row)?;

    // Collect the streamed data into a vector
    let orders: Vec<Orders> = stream.collect::<Result<Vec<Orders>, _>>()?;

    // Create a hashmap of orders using the orderkey as the key
    let orders_map: HashMap<i32, &Orders> = orders
        .iter()
        .map(|order| (order.o_orderkey, order))
        .collect();

    // Join the sample data and orders data to generate s2_sample
    let s2_sample: Vec<S2Sample> = sample
        .iter()
        .filter_map(|lineitem| {
            orders_map.get(&lineitem.l_orderkey).map(|order| S2Sample {
                o_orderkey: lineitem.l_orderkey,
                l_partkey: lineitem.l_partkey,
                l_suppkey: lineitem.l_suppkey,
                l_linenumber: lineitem.l_linenumber,
                l_quantity: lineitem.l_quantity,
                l_extendedprice: lineitem.l_extendedprice,
                l_discount: lineitem.l_discount,
                l_tax: lineitem.l_tax,
                l_returnflag: lineitem.l_returnflag.clone(),
                l_linestatus: lineitem.l_linestatus.clone(),
                l_shipdate: lineitem.l_shipdate.clone(),
                l_commitdate: lineitem.l_commitdate.clone(),
                l_receiptdate: lineitem.l_receiptdate.clone(),
                l_shipinstruct: lineitem.l_shipinstruct.clone(),
                l_shipmode: lineitem.l_shipmode.clone(),
                l_comment: lineitem.l_comment.clone(),
                o_custkey: order.o_custkey,
                o_orderstatus: order.o_orderstatus.clone(),
                o_totalprice: order.o_totalprice,
                o_orderdate: order.o_orderdate.clone(),
                o_orderpriority: order.o_orderpriority.clone(),
                o_clerk: order.o_clerk.clone(),
                o_shippriority: order.o_shippriority,
                o_comment: order.o_comment.clone(),
            })
        })
        .collect();

    // let end_time = Instant::now(); // Stop measuring time
    // let execution_time = end_time - start_time;

    // println!(
    //     "Execution time S2sample: {:.3}",
    //     execution_time.as_secs_f64()
    // );

    Ok(s2_sample)
}

//function to generate s2sample
// pub fn generate_s2_sample(
//     conn: &Connection,
//     samples: &[S1Sample],
// ) -> Result<Vec<S2Sample>, Box<dyn Error>> {
//     let mut orders: DashMap<i32, Orders> = DashMap::with_capacity(1_000_000);

//     let mut stmt = conn.prepare("SELECT * FROM orders")?;

//     let mut stream = stmt.query_map([], |row| {
//         let order = Orders::from_row(row)?;
//         orders.insert(order.o_orderkey, order);
//         Ok(())
//     })?;

//     stream.collect::<Result<()>>()?;

//     let s2_samples: Vec<S2Sample> = samples
//         .par_iter()
//         .filter_map(|s| {
//             if let Some(order) = orders.get(&s.l_orderkey) {
//                 Some(S2Sample {
//                     o_orderkey: s.l_orderkey,
//                     l_partkey: s.l_partkey,
//                     l_suppkey: s.l_suppkey,
//                     l_linenumber: s.l_linenumber,
//                     l_quantity: s.l_quantity,
//                     l_extendedprice: s.l_extendedprice,
//                     l_discount: s.l_discount,
//                     l_tax: s.l_tax,
//                     l_returnflag: s.l_returnflag.clone(),
//                     l_linestatus: s.l_linestatus.clone(),
//                     l_shipdate: s.l_shipdate.clone(),
//                     l_commitdate: s.l_commitdate.clone(),
//                     l_receiptdate: s.l_receiptdate.clone(),
//                     l_shipinstruct: s.l_shipinstruct.clone(),
//                     l_shipmode: s.l_shipmode.clone(),
//                     l_comment: s.l_comment.clone(),
//                     o_custkey: order.o_custkey,
//                     o_orderstatus: order.o_orderstatus.clone(),
//                     o_totalprice: order.o_totalprice,
//                     o_orderdate: order.o_orderdate.clone(),
//                     o_orderpriority: order.o_orderpriority.clone(),
//                     o_clerk: order.o_clerk.clone(),
//                     o_shippriority: order.o_shippriority,
//                     o_comment: String::from(&order.o_comment), // Reuse string
//                 })
//             } else {
//                 None
//             }
//         })
//         .collect();

//     Ok(s2_samples)
// }

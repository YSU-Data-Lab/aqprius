use rusqlite::{params, Connection, Result};

pub fn create_sample_tables(conn: &Connection, sample_fraction: f64) -> Result<()> {
    // Drop existing sample tables if they exist
    conn.execute("DROP TABLE IF EXISTS s1_sample", params![])?;
    conn.execute("DROP TABLE IF EXISTS s2_sample", params![])?;
    conn.execute("DROP TABLE IF EXISTS s3_sample", params![])?;
    conn.execute("DROP TABLE IF EXISTS s4_sample", params![])?;
    conn.execute("DROP TABLE IF EXISTS s5_sample", params![])?;

    // Create s1_sample table structure
    conn.execute(
        "CREATE TABLE s1_sample AS SELECT * FROM lineitem WHERE 1=0",
        params![],
    )?;

    // Generate shuffled row IDs
    conn.execute(
        "CREATE TEMP TABLE ids AS SELECT rowid FROM lineitem ORDER BY RANDOM()",
        params![],
    )?;

    // Calculate the number of rows to sample
    let total_rows: i64 =
        conn.query_row("SELECT COUNT(*) FROM lineitem", params![], |row| row.get(0))?;
    let sample_size = (sample_fraction * total_rows as f64).round() as i64;

    // Take first N shuffled IDs as sample
    conn.execute(
        "INSERT INTO s1_sample
         SELECT * FROM lineitem
         WHERE rowid IN (SELECT rowid FROM ids LIMIT ?)",
        params![sample_size],
    )?;
    println!("s1_sample table created with sampled data.");
    // Join s1_sample with orders table to create s2_sample
    conn.execute(
        "CREATE TABLE IF NOT EXISTS s2_sample AS
         SELECT s1.*, orders.*
         FROM s1_sample AS s1
         JOIN orders ON s1.l_orderkey = orders.o_orderkey",
        params![],
    )?;
    println!("s2_sample table created with joined data.");

    // Join s2_sample with customer table to create s3_sample
    conn.execute(
        "CREATE TABLE IF NOT EXISTS s3_sample AS
         SELECT s2.*, customer.*
         FROM s2_sample AS s2
         JOIN customer ON s2.o_custkey = customer.c_custkey",
        params![],
    )?;
    println!("s3_sample table created with joined data.");

    // Join s3_sample with nation table to create s4_sample
    conn.execute(
        "CREATE TABLE IF NOT EXISTS s4_sample AS
         SELECT s3.*, nation.*
         FROM s3_sample AS s3
         JOIN nation ON s3.c_nationkey = nation.n_nationkey",
        params![],
    )?;
    println!("s4_sample table created with joined data.");

    // Join s4_sample with region table to create s5_sample
    conn.execute(
        "CREATE TABLE IF NOT EXISTS s5_sample AS
         SELECT s4.*, region.*
         FROM s4_sample AS s4
         JOIN region ON s4.n_regionkey = region.r_regionkey",
        params![],
    )?;
    println!("s5_sample table created with joined data.");

    Ok(())
}

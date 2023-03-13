use polars::lazy::prelude::*;
use polars::prelude::*;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let f = "data.txt";
    let df = CsvReader::from_path(f)
        .expect("error reading csv")
        .has_header(true)
        .with_delimiter('\t' as u8)
        .finish()
        .unwrap();

    dbg!(df.head(Some(10)));
    //df.filter(col("context").eq(lit("AG")));

    let r1 = col("FR")
        .eq(lit("f"))
        .and(col("read12").eq(lit("r1")))
        .and(col("bq_bin").eq(lit("60+")));

    let e = df
        .lazy()
        .filter(r1)
        .groupby(["context"])
        .agg([
            count(),
            col("total_count"),
            col("error_count"),
            col("read_pos"),
        ])
        .sort("context", SortOptions::default())
        .collect()?;

    eprintln!("{:?}", e);
    for rowi in 0..e.shape().1 {
        let row = e.get_row(rowi).expect("error extracting row {rowi}");

        eprintln!("row: {:?}", row);
        break;
    }

    //e.iter().for_each(|r| eprintln!("{:?}", r));
    //eprintln!("OK");
    //eprintln!("{:?}", e);
    Ok(())
}

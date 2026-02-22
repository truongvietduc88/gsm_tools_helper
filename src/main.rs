mod export_from_excel_to_parquet;

use anyhow::Result;

fn main() -> Result<()> {
    export_from_excel_to_parquet::run()
}
from pathlib import Path
import polars as pl
from lakelogic import DataProcessor


def main() -> None:
    """Run the customer onboarding example contract locally."""
    base_dir = Path(__file__).resolve().parent
    data_path = base_dir / "data" / "customers.csv"
    contract_path = base_dir / "contract.yaml"

    df = pl.read_csv(data_path)
    processor = DataProcessor(engine="duckdb", contract=contract_path)
    good_df, bad_df = processor.run(df)

    print(f"Good records: {len(good_df)}")
    print(f"Quarantined records: {len(bad_df)}")

    good_df.write_csv(base_dir / "good_customers.csv")
    bad_df.write_csv(base_dir / "bad_customers.csv")


if __name__ == "__main__":
    main()

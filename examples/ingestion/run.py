from pathlib import Path
import polars as pl
from lakeguard import DataProcessor


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    data_path = base_dir / "raw_crm.csv"
    contract_path = base_dir / "contract.yaml"

    df = pl.read_csv(data_path)
    processor = DataProcessor(engine="polars", contract=contract_path)
    good_df, bad_df = processor.run(df)

    print(f"Good records: {len(good_df)}")
    print(f"Quarantined records: {len(bad_df)}")

    good_df.write_csv(base_dir / "good_crm.csv")
    bad_df.write_csv(base_dir / "bad_crm.csv")


if __name__ == "__main__":
    main()

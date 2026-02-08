from pathlib import Path
import polars as pl
from lakeguard import DataProcessor


def main() -> None:
    """Run the external notebook logic example."""
    base_dir = Path(__file__).resolve().parent
    data_path = base_dir / "data" / "sales.csv"
    contract_path = base_dir / "contract_notebook.yaml"

    df = pl.read_csv(data_path)
    processor = DataProcessor(engine="polars", contract=contract_path)
    good_df, bad_df = processor.run(df, source_path=str(data_path), materialize=True)

    print(f"Good records: {len(good_df)}")
    print(f"Quarantined records: {len(bad_df)}")


if __name__ == "__main__":
    main()

from loguru import logger
from typing import List, Dict, Any, Literal
from faker import Faker

import pandas as pd
import polars as pl
import random

# --- FAKE DATA GENERATION MAPPING ---
# --- CONSTANTS ---
FAKER = Faker()
CURRENCY_CODES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD"]
BROKER_CODES = ["INTERACTIVE_BROKERS", "TRADING212", "METAAPI"]
ASSET_TYPES = ["FX", "FUTURES", "STOCK", "INDEX"]
TIMEFRAMES = ["5m", "15m", "30m", "60m"]
NULL_PROBABILITY = 0.05  # 5% chance of a nullable column being None


# --- FAKER DATA GENERATION MAPPING (UPPERCASE for Schema Alignment) ---
FAKER_MAPPING = {
    # General Types
    "STRING:UUID": FAKER.uuid4,
    "STRING:EMAIL": FAKER.email,
    "TIMESTAMP": FAKER.date_time_this_year,
    "DATE": FAKER.date_this_year,
    # Numerics
    "FLOAT64": lambda: round(random.uniform(1.0, 1000.0), 6),
    "FLOAT": lambda: round(random.uniform(1.0, 1000.0), 6),
    "INT64": FAKER.random_int,
    "INT": FAKER.random_int,
    "BOOLEAN": FAKER.boolean,
    "STRING": FAKER.word,
    # Specific Trading Mappings
    "STRING:INTERNAL_SYMBOL": lambda: random.choice(
        ["EURUSD", "SPX.FUT", "AAPL.STK", "CRUDE.FUT"]
    ),
    "STRING:BROKER_API_SOURCE": lambda: random.choice(BROKER_CODES),
    "STRING:CONTRACT_TYPE": lambda: random.choice(ASSET_TYPES),
    "STRING:TIMEFRAME": lambda: random.choice(TIMEFRAMES),
    "STRING:IB_EXCHANGE_CODE": lambda: random.choice(["GLOBEX", "IDEALPRO", "NASDAQ"]),
}


# ----------------------------------------------------------------
def generate_fake_tables(
    schemas: List[Dict[str, Any]],  # CHANGED INPUT TYPE: List of schema dictionaries
    num_rows: int = 10,
    engine: Literal["pandas", "polars"] = "polars",
) -> Dict[str, Any]:
    """
    Generates fake DataFrames (Pandas or Polars) based on a list of fully loaded schema dictionaries.

    This function eliminates file system access and focuses purely on data generation logic,
    making it ideal for unit testing and ETL pipeline setup.

    Args:
        schemas (List[Dict[str, Any]]): A list of fully loaded schema dictionaries.
        num_rows (int): The number of fake data rows to generate for each table.
        engine (Literal["pandas", "polars"]): The type of DataFrame to return ('polars' or 'pandas').

    Returns:
        Dict[str, Any]: A dictionary where keys are table names and values are
            the generated DataFrames.

    💡 Usage Example (Polars):

    ```python
    from your_data_generator import generate_fake_tables

    # Schemas must be loaded externally first (e.g., using a YAML loader utility)
    TICKER_SCHEMA = {'dataset': 'ticker_registry', 'model': {...}, 'primary_key': ...}

    fake_data = generate_fake_tables(
        schemas=[TICKER_SCHEMA], # Pass the dictionary directly
        num_rows=5,
        engine='polars'
    )
    print(fake_data['ticker_registry'].head())
    ```
    """

    # --- Nested Function: Generates Data (Logic remains functional) ---
    def generate_table(
        schema: dict, parent_data: Dict[str, Any], num_rows: int
    ) -> pd.DataFrame:
        """Generates a Pandas DataFrame using Faker mappings and foreign key lookups."""
        data = {}
        cols = schema["model"]["columns"]
        pk_columns = []
        pks = schema.get("primary_key", [])
        if isinstance(pks, str):
            pk_columns = [pks]
        elif isinstance(pks, list):
            pk_columns = pks

        # 1. Handle referential integrity (FKs)
        fk_map = {}
        for fk in schema.get("foreign_keys", []):
            ref_table = fk["ref_table"]
            ref_column = fk["ref_column"]

            if parent_data and ref_table in parent_data:
                parent_df = parent_data[ref_table]

                if isinstance(parent_df, pl.DataFrame):
                    parent_keys = parent_df[ref_column].to_list()
                else:
                    parent_keys = parent_df[ref_column].tolist()

                fk_map[fk["column"]] = parent_keys

        # 2. Generate data for all columns
        for col in cols:
            col_name = col["name"]
            col_type_upper = col["type"].upper()
            is_nullable = col.get("nullable", True)

            key = f"{col_type_upper}:{col_name.upper()}"
            generator = FAKER_MAPPING.get(
                key, FAKER_MAPPING.get(col_type_upper, lambda: None)
            )

            col_data = []
            for _ in range(num_rows):
                val = None

                # Generation Logic
                if col_name == "Currency":
                    val = random.choice(CURRENCY_CODES)
                elif col_name in fk_map and fk_map[col_name]:
                    val = random.choice(fk_map[col_name])
                elif generator is not None:
                    val = generator()

                # Apply Nullability Check
                is_pk = col_name in pk_columns

                if is_nullable and not is_pk and random.random() < NULL_PROBABILITY:
                    col_data.append(None)
                else:
                    if not is_nullable and val is None:
                        raise ValueError(
                            f"Non-nullable column '{col_name}' (type {col['type']}) failed to generate a value."
                        )
                    col_data.append(val)

            data[col_name] = col_data

        return pd.DataFrame(data)

    # --- Main Execution ---
    # tables = {}

    # # 1. Input is already the list of loaded schemas (renamed from schema_configs)
    # loaded_schemas = schemas

    # # 2. Sort schemas by dependencies
    # sorted_schemas = sorted(loaded_schemas, key=lambda s: len(s.get("foreign_keys", [])))

    # # 3. Generate all tables as Pandas DataFrames
    # for schema in sorted_schemas:
    #     table_name = schema["dataset"]
    #     tables[table_name] = generate_table(schema, parent_data=tables, num_rows=num_rows)

    #     # Attach the schema definition to the table for later use (e.g., in Step 4)
    #     tables[table_name].attrs['schema_def'] = schema

    # # 4. Convert to Polars and Apply Time Zone (The New Logic)
    # if engine == "polars":
    #     for name, df in tables.items():
    #         df_pl = pl.from_pandas(df)
    #         schema_def = df.attrs['schema_def']

    #         # --- Identify and Fix Timestamp Columns ---
    #         timestamp_cols = []
    #         for col_def in schema_def['model']['columns']:
    #             if col_def['type'].upper() in ['TIMESTAMP', 'DATETIME']:
    #                 timestamp_cols.append(col_def['name'])

    #         if timestamp_cols:
    #             logger.info(f"Applying UTC timezone replacement to columns: {timestamp_cols} in table {name}")

    #             df_pl = df_pl.with_columns(
    #                 [
    #                     pl.col(col_name).dt.replace_time_zone("UTC")
    #                     for col_name in timestamp_cols
    #                 ]
    #             )

    #         tables[name] = df_pl

    # return tables

    tables = {}

    # 1. Load all schemas... (Assuming this part is handled correctly above)
    loaded_schemas = schemas

    # 2. Sort schemas by dependencies...
    sorted_schemas = sorted(
        loaded_schemas, key=lambda s: len(s.get("foreign_keys", []))
    )

    # 3. Generate all tables
    for schema in sorted_schemas:
        target_table_name = schema["dataset"]

        # Generate the Pandas DataFrame
        tables[target_table_name] = generate_table(
            schema, parent_data=tables, num_rows=num_rows
        )

        # Attach the schema definition to the table for later use
        tables[target_table_name].attrs["schema_def"] = schema

    # 4. Convert to Polars and Apply Time Zone (The New Logic)
    if engine == "polars":
        # The loop must use the assigned table names (which could be target_table_name)
        for name, df in tables.items():
            df_pl = pl.from_pandas(df)
            schema_def = df.attrs["schema_def"]

            # --- Identify and Fix Timestamp Columns ---
            timestamp_cols = []
            for col_def in schema_def["model"]["columns"]:
                if col_def["type"].upper() in ["TIMESTAMP", "DATETIME"]:
                    timestamp_cols.append(col_def["name"])

            if timestamp_cols:
                logger.info(
                    f"Applying UTC timezone replacement to columns: {timestamp_cols} in table {name}"
                )

                df_pl = df_pl.with_columns(
                    [
                        pl.col(col_name).dt.replace_time_zone("UTC")
                        for col_name in timestamp_cols
                    ]
                )

            tables[name] = df_pl

    return tables

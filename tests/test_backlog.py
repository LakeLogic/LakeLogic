"""
Tests for the backlog features:
  1. GDPR (forget/mask_pii)
  2. Date dimension generation
  3. Polars streaming (run_source_streaming)
  4. Partition-aware merge materialization
"""

import pytest

from lakelogic.core.models import (
    DataContract,
    FieldDefinition,
    Info,
    Materialization,
    Model,
    Server,
)

# ── GDPR Tests ───────────────────────────────────────────────────────────────


class TestGDPRForget:
    def _make_contract_with_pii(self):
        return DataContract(
            version="1.0",
            info=Info(title="Customer Data", version="1.0"),
            model=Model(
                fields=[
                    FieldDefinition(name="customer_id", type="string", required=True),
                    FieldDefinition(name="email", type="string", pii=True),
                    FieldDefinition(name="full_name", type="string", pii=True),
                    FieldDefinition(name="order_total", type="float"),
                ]
            ),
        )

    def test_forget_nullify_polars(self):
        import polars as pl

        from lakelogic.core.gdpr import forget_subjects

        contract = self._make_contract_with_pii()
        df = pl.DataFrame(
            {
                "customer_id": ["c1", "c2", "c3"],
                "email": ["a@x.com", "b@x.com", "c@x.com"],
                "full_name": ["Alice", "Bob", "Charlie"],
                "order_total": [100.0, 200.0, 300.0],
            }
        )

        result = forget_subjects(df, contract, "customer_id", ["c1", "c3"])

        # c1 and c3 should have PII nullified
        assert result["email"][0] is None  # c1
        assert result["email"][1] == "b@x.com"  # c2 untouched
        assert result["email"][2] is None  # c3
        assert result["full_name"][0] is None
        assert result["full_name"][1] == "Bob"
        assert result["full_name"][2] is None
        # Non-PII should be untouched
        assert result["order_total"].to_list() == [100.0, 200.0, 300.0]

    def test_forget_hash_polars(self):
        import polars as pl

        from lakelogic.core.gdpr import forget_subjects

        contract = self._make_contract_with_pii()
        df = pl.DataFrame(
            {
                "customer_id": ["c1", "c2"],
                "email": ["a@x.com", "b@x.com"],
                "full_name": ["Alice", "Bob"],
                "order_total": [100.0, 200.0],
            }
        )

        result = forget_subjects(df, contract, "customer_id", ["c1"], erasure_strategy="hash")

        # c1's email should be hashed (64 hex chars for SHA-256)
        assert len(result["email"][0]) == 64
        assert result["email"][1] == "b@x.com"  # c2 untouched

    def test_forget_redact_polars(self):
        import polars as pl

        from lakelogic.core.gdpr import forget_subjects

        contract = self._make_contract_with_pii()
        df = pl.DataFrame(
            {
                "customer_id": ["c1", "c2"],
                "email": ["a@x.com", "b@x.com"],
                "full_name": ["Alice", "Bob"],
                "order_total": [100.0, 200.0],
            }
        )

        result = forget_subjects(df, contract, "customer_id", ["c1"], erasure_strategy="redact")

        assert result["email"][0] == "***REDACTED***"
        assert result["full_name"][0] == "***REDACTED***"
        assert result["email"][1] == "b@x.com"

    def test_forget_nullify_pandas(self):
        import pytest

        pytest.skip("pandas")
        import pandas as pd

        from lakelogic.core.gdpr import forget_subjects

        contract = self._make_contract_with_pii()
        df = pd.DataFrame(
            {
                "customer_id": ["c1", "c2", "c3"],
                "email": ["a@x.com", "b@x.com", "c@x.com"],
                "full_name": ["Alice", "Bob", "Charlie"],
                "order_total": [100.0, 200.0, 300.0],
            }
        )

        result = forget_subjects(df, contract, "customer_id", ["c2"])

        assert pd.isna(result.loc[1, "email"])  # c2 nullified
        assert pd.isna(result.loc[1, "full_name"])
        assert result.loc[0, "email"] == "a@x.com"  # c1 untouched
        assert result.loc[2, "email"] == "c@x.com"  # c3 untouched
        # Non-PII untouched
        assert list(result["order_total"]) == [100.0, 200.0, 300.0]

    def test_forget_nonexistent_subject(self):
        import polars as pl

        from lakelogic.core.gdpr import forget_subjects

        contract = self._make_contract_with_pii()
        df = pl.DataFrame(
            {
                "customer_id": ["c1", "c2"],
                "email": ["a@x.com", "b@x.com"],
                "full_name": ["Alice", "Bob"],
                "order_total": [100.0, 200.0],
            }
        )

        result = forget_subjects(df, contract, "customer_id", ["c999"])
        # No changes
        assert result["email"].to_list() == ["a@x.com", "b@x.com"]

    def test_forget_missing_subject_column_raises(self):
        import polars as pl

        from lakelogic.core.gdpr import forget_subjects

        contract = self._make_contract_with_pii()
        df = pl.DataFrame({"email": ["a@x.com"], "full_name": ["Alice"], "order_total": [100.0]})

        with pytest.raises(ValueError, match="Subject column"):
            forget_subjects(df, contract, "customer_id", ["c1"])

    def test_forget_no_pii_fields_warns(self):
        import polars as pl

        from lakelogic.core.gdpr import forget_subjects

        contract = DataContract(
            version="1.0",
            model=Model(
                fields=[
                    FieldDefinition(name="id", type="int"),
                    FieldDefinition(name="value", type="float"),
                ]
            ),
        )
        df = pl.DataFrame({"id": [1, 2], "value": [10.0, 20.0]})
        result = forget_subjects(df, contract, "id", [1])
        # Returns unchanged
        assert result["value"].to_list() == [10.0, 20.0]

    def test_invalid_strategy_raises(self):
        import polars as pl

        from lakelogic.core.gdpr import forget_subjects

        contract = self._make_contract_with_pii()
        df = pl.DataFrame(
            {
                "customer_id": ["c1"],
                "email": ["a@x.com"],
                "full_name": ["Alice"],
                "order_total": [100.0],
            }
        )
        with pytest.raises(ValueError, match="Invalid erasure_strategy"):
            forget_subjects(df, contract, "customer_id", ["c1"], erasure_strategy="delete")


class TestGDPRMask:
    def _make_contract_with_pii(self):
        return DataContract(
            version="1.0",
            model=Model(
                fields=[
                    FieldDefinition(name="customer_id", type="string"),
                    FieldDefinition(name="email", type="string", pii=True),
                    FieldDefinition(name="phone", type="string", pii=True),
                    FieldDefinition(name="value", type="float"),
                ]
            ),
        )

    def test_mask_nullify_polars(self):
        import polars as pl

        from lakelogic.core.gdpr import mask_pii_columns

        contract = self._make_contract_with_pii()
        df = pl.DataFrame(
            {
                "customer_id": ["c1", "c2"],
                "email": ["a@x.com", "b@x.com"],
                "phone": ["111", "222"],
                "value": [10.0, 20.0],
            }
        )

        result = mask_pii_columns(df, contract, strategy="nullify")
        assert result["email"][0] is None
        assert result["email"][1] is None
        assert result["phone"][0] is None
        assert result["customer_id"].to_list() == ["c1", "c2"]  # Not PII
        assert result["value"].to_list() == [10.0, 20.0]

    def test_mask_hash_preserves_referential_integrity(self):
        import polars as pl

        from lakelogic.core.gdpr import mask_pii_columns

        contract = self._make_contract_with_pii()
        df = pl.DataFrame(
            {
                "customer_id": ["c1", "c2", "c1"],
                "email": ["a@x.com", "b@x.com", "a@x.com"],
                "phone": ["111", "222", "111"],
                "value": [10.0, 20.0, 30.0],
            }
        )

        result = mask_pii_columns(df, contract, strategy="hash")
        # Same emails should hash to same value (preserves joins)
        assert result["email"][0] == result["email"][2]
        assert result["email"][0] != result["email"][1]

    def test_mask_pandas(self):
        import pytest

        pytest.skip("pandas")
        import pandas as pd

        from lakelogic.core.gdpr import mask_pii_columns

        contract = self._make_contract_with_pii()
        df = pd.DataFrame(
            {
                "customer_id": ["c1", "c2"],
                "email": ["a@x.com", "b@x.com"],
                "phone": ["111", "222"],
                "value": [10.0, 20.0],
            }
        )

        result = mask_pii_columns(df, contract, strategy="redact")
        assert result.loc[0, "email"] == "***REDACTED***"
        assert result.loc[0, "phone"] == "***REDACTED***"
        assert list(result["value"]) == [10.0, 20.0]

    def test_mask_with_custom_columns(self):
        import polars as pl

        from lakelogic.core.gdpr import mask_pii_columns

        contract = self._make_contract_with_pii()
        df = pl.DataFrame(
            {
                "customer_id": ["c1"],
                "email": ["a@x.com"],
                "phone": ["111"],
                "value": [10.0],
            }
        )

        # Only mask email, not phone
        result = mask_pii_columns(df, contract, columns=["email"])
        assert result["email"][0] is None
        assert result["phone"][0] == "111"  # Not masked


class TestGDPRAuditReport:
    def test_erasure_report(self):
        from lakelogic.core.gdpr import generate_erasure_report

        contract = DataContract(
            version="1.0",
            info=Info(title="Customer Data", version="1.0"),
            model=Model(
                fields=[
                    FieldDefinition(name="email", type="string", pii=True),
                ]
            ),
        )
        report = generate_erasure_report(contract, "customer_id", ["c1", "c2"], affected_rows=5)
        assert report["report_type"] == "gdpr_erasure"
        assert report["subjects_erased"] == 2
        assert report["affected_rows"] == 5
        assert "email" in report["pii_columns_affected"]
        assert "Article 17" in report["compliance_note"]


# ── Date Dimension Tests ─────────────────────────────────────────────────────


class TestDateDimension:
    def test_basic_generation_polars(self):
        from lakelogic.core.dim_date import generate_date_dimension

        df = generate_date_dimension(
            start_date="2024-01-01",
            end_date="2024-01-31",
            engine="polars",
        )
        assert df.height == 31  # 31 days in January 2024
        assert "date_key" in df.columns
        assert "full_date" in df.columns
        assert "day_name" in df.columns
        assert "is_weekend" in df.columns
        assert "fiscal_year" in df.columns

    def test_basic_generation_pandas(self):
        import pytest

        pytest.skip("pandas")
        from lakelogic.core.dim_date import generate_date_dimension

        df = generate_date_dimension(
            start_date="2024-06-01",
            end_date="2024-06-30",
            engine="pandas",
        )
        assert len(df) == 30  # 30 days in June

    def test_date_key_format(self):
        from lakelogic.core.dim_date import generate_date_dimension

        df = generate_date_dimension(
            start_date="2024-03-15",
            end_date="2024-03-15",
            engine="polars",
        )
        assert df["date_key"][0] == 20240315

    def test_day_of_week(self):
        from lakelogic.core.dim_date import generate_date_dimension

        df = generate_date_dimension(
            start_date="2024-01-01",  # Monday
            end_date="2024-01-07",  # Sunday
            engine="polars",
        )
        day_names = df["day_name"].to_list()
        assert day_names == ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    def test_weekend_flags(self):
        from lakelogic.core.dim_date import generate_date_dimension

        df = generate_date_dimension(
            start_date="2024-01-01",
            end_date="2024-01-07",
            holiday_calendar="none",
            engine="polars",
        )
        weekends = df["is_weekend"].to_list()
        # Mon-Fri = False, Sat-Sun = True
        assert weekends == [False, False, False, False, False, True, True]

    def test_us_holidays(self):
        from lakelogic.core.dim_date import generate_date_dimension

        df = generate_date_dimension(
            start_date="2024-01-01",
            end_date="2024-12-31",
            holiday_calendar="us",
            engine="polars",
        )
        holidays = df.filter(df["is_holiday"] == True)
        holiday_names = holidays["holiday_name"].to_list()
        assert "New Year's Day" in holiday_names
        assert "Independence Day" in holiday_names
        assert "Thanksgiving" in holiday_names
        assert "Christmas Day" in holiday_names

    def test_uk_holidays(self):
        from lakelogic.core.dim_date import generate_date_dimension

        df = generate_date_dimension(
            start_date="2024-01-01",
            end_date="2024-12-31",
            holiday_calendar="uk",
            engine="polars",
        )
        holidays = df.filter(df["is_holiday"] == True)
        holiday_names = holidays["holiday_name"].to_list()
        assert "Good Friday" in holiday_names
        assert "Boxing Day" in holiday_names

    def test_fiscal_year_april_start(self):
        from lakelogic.core.dim_date import generate_date_dimension

        df = generate_date_dimension(
            start_date="2024-01-01",
            end_date="2024-12-31",
            fiscal_year_start_month=4,
            engine="polars",
        )
        # January 2024 → FY2023 (fiscal year started Apr 2023)
        jan_row = df.filter(df["date_key"] == 20240115)
        assert jan_row["fiscal_year"][0] == 2023
        assert jan_row["fiscal_quarter"][0] == 4  # Jan-Mar = Q4 in Apr fiscal year

        # April 2024 → FY2024 (new fiscal year starts)
        apr_row = df.filter(df["date_key"] == 20240415)
        assert apr_row["fiscal_year"][0] == 2024
        assert apr_row["fiscal_quarter"][0] == 1

    def test_custom_holidays(self):
        from lakelogic.core.dim_date import generate_date_dimension

        df = generate_date_dimension(
            start_date="2024-03-01",
            end_date="2024-03-31",
            holiday_calendar="none",
            custom_holidays={"2024-03-17": "St Patrick's Day"},
            engine="polars",
        )
        mar17 = df.filter(df["date_key"] == 20240317)
        assert mar17["is_holiday"][0] == True
        assert mar17["holiday_name"][0] == "St Patrick's Day"

    def test_month_start_end_flags(self):
        from lakelogic.core.dim_date import generate_date_dimension

        df = generate_date_dimension(
            start_date="2024-02-01",
            end_date="2024-02-29",  # 2024 is a leap year
            engine="polars",
        )
        assert df["is_month_start"][0] == True
        assert df["is_month_end"][-1] == True
        assert df.height == 29  # Leap year

    def test_relative_flags(self):
        from lakelogic.core.dim_date import generate_date_dimension

        df = generate_date_dimension(
            start_date="2020-01-01",
            end_date="2030-12-31",
            include_relative_flags=True,
            engine="polars",
        )
        assert "is_today" in df.columns
        assert "is_current_month" in df.columns
        assert "days_from_today" in df.columns

    def test_duckdb_output(self):
        import pytest

        pytest.skip("duckdb")
        import duckdb

        from lakelogic.core.dim_date import generate_date_dimension

        con = duckdb.connect()
        try:
            result = generate_date_dimension(
                start_date="2024-01-01",
                end_date="2024-01-31",
                engine="duckdb",
                table_name="test_dim_date",
                connection=con,
            )
            assert result == "test_dim_date"
            count = con.execute("SELECT count(*) FROM test_dim_date").fetchone()[0]
            assert count == 31
        finally:
            con.close()

    def test_invalid_dates_raises(self):
        from lakelogic.core.dim_date import generate_date_dimension

        with pytest.raises(ValueError, match="must be before"):
            generate_date_dimension(start_date="2025-01-01", end_date="2020-01-01")

    def test_iso_week_columns(self):
        from lakelogic.core.dim_date import generate_date_dimension

        df = generate_date_dimension(
            start_date="2024-01-01",
            end_date="2024-01-01",
            engine="polars",
        )
        assert "iso_year" in df.columns
        assert "iso_week" in df.columns
        assert "iso_weekday" in df.columns

    def test_quarter_columns(self):
        from lakelogic.core.dim_date import generate_date_dimension

        df = generate_date_dimension(
            start_date="2024-01-01",
            end_date="2024-12-31",
            engine="polars",
        )
        # Q1: Jan-Mar, Q2: Apr-Jun, Q3: Jul-Sep, Q4: Oct-Dec
        jan = df.filter(df["date_key"] == 20240115)
        assert jan["quarter"][0] == 1
        assert jan["year_quarter"][0] == "2024-Q1"

        jul = df.filter(df["date_key"] == 20240715)
        assert jul["quarter"][0] == 3


# ── Polars Streaming Tests ───────────────────────────────────────────────────


class TestPolarsStreaming:
    def test_streaming_requires_polars_engine(self, tmp_path):
        import yaml

        contract_data = {
            "version": "1.0",
            "info": {"title": "Test", "version": "1.0"},
            "server": {"type": "file", "path": str(tmp_path / "data.parquet"), "format": "parquet"},
        }
        contract_file = tmp_path / "contract.yaml"
        with open(contract_file, "w") as f:
            yaml.dump(contract_data, f)

        from lakelogic.core.processor import DataProcessor

        proc = DataProcessor(str(contract_file), engine="polars")
        proc.engine_name = "duckdb"
        with pytest.raises(ValueError, match="polars"):
            proc.run_source_streaming()

    def test_streaming_parquet(self, tmp_path):
        import polars as pl
        import yaml

        # Create test data
        test_data = pl.DataFrame(
            {
                "id": list(range(100)),
                "value": [float(i * 10) for i in range(100)],
                "category": ["A" if i % 2 == 0 else "B" for i in range(100)],
            }
        )
        data_path = tmp_path / "data.parquet"
        test_data.write_parquet(str(data_path))

        contract_data = {
            "version": "1.0",
            "info": {"title": "Streaming Test", "version": "1.0"},
            "server": {"type": "file", "path": str(data_path), "format": "parquet"},
            "source": {"type": "file", "path": str(data_path)},
        }
        contract_file = tmp_path / "contract.yaml"
        with open(contract_file, "w") as f:
            yaml.dump(contract_data, f)

        from lakelogic.core.processor import DataProcessor

        proc = DataProcessor(str(contract_file), engine="polars")

        result = proc.run_source_streaming(str(data_path))
        good = result.good
        if isinstance(good, pl.LazyFrame):
            good = good.collect()
        assert good.height == 100

    def test_streaming_sink_to_parquet(self, tmp_path):
        import polars as pl
        import yaml

        test_data = pl.DataFrame(
            {
                "id": list(range(50)),
                "value": [float(i) for i in range(50)],
            }
        )
        data_path = tmp_path / "input.parquet"
        test_data.write_parquet(str(data_path))

        contract_data = {
            "version": "1.0",
            "info": {"title": "Sink Test", "version": "1.0"},
            "server": {"type": "file", "path": str(data_path), "format": "parquet"},
            "source": {"type": "file", "path": str(data_path)},
        }
        contract_file = tmp_path / "contract.yaml"
        with open(contract_file, "w") as f:
            yaml.dump(contract_data, f)

        from lakelogic.core.processor import DataProcessor

        proc = DataProcessor(str(contract_file), engine="polars")

        output = tmp_path / "output.parquet"
        result = proc.run_source_streaming(str(data_path), output_path=str(output))

        assert "target" in result
        # Verify the file was written
        readback = pl.read_parquet(str(output))
        assert readback.height == 50

    def test_streaming_csv(self, tmp_path):
        import polars as pl
        import yaml

        test_data = pl.DataFrame(
            {
                "id": list(range(20)),
                "name": [f"item_{i}" for i in range(20)],
            }
        )
        data_path = tmp_path / "data.csv"
        test_data.write_csv(str(data_path))

        contract_data = {
            "version": "1.0",
            "info": {"title": "CSV Stream", "version": "1.0"},
            "server": {"type": "file", "path": str(data_path), "format": "csv"},
            "source": {"type": "file", "path": str(data_path)},
        }
        contract_file = tmp_path / "contract.yaml"
        with open(contract_file, "w") as f:
            yaml.dump(contract_data, f)

        from lakelogic.core.processor import DataProcessor

        proc = DataProcessor(str(contract_file), engine="polars")

        result = proc.run_source_streaming(str(data_path))
        good = result.good
        if isinstance(good, pl.LazyFrame):
            good = good.collect()
        assert good.height == 20


# ── Partition-Aware Merge Tests ──────────────────────────────────────────────


@pytest.mark.skip(reason="Requires pandas (deprecated execution engine integration)")
class TestPartitionAwareMerge:
    def test_partitioned_merge_creates_partition_dirs(self, tmp_path):
        import pandas as pd

        from lakelogic.core.materialization import materialize_dataframe

        contract = DataContract(
            version="1.0",
            primary_key=["id"],
            materialization=Materialization(
                strategy="merge",
                partition_by=["region"],
                target_path=str(tmp_path / "output"),
                format="parquet",
            ),
            server=Server(type="file", path=str(tmp_path / "output"), format="parquet"),
        )

        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "region": ["US", "EU", "US", "EU"],
                "value": [10, 20, 30, 40],
            }
        )

        result = materialize_dataframe(df, contract)
        assert result["rows_written"] == 4

        # Check partition directories
        us_dir = tmp_path / "output" / "region=US"
        eu_dir = tmp_path / "output" / "region=EU"
        assert us_dir.exists()
        assert eu_dir.exists()
        assert (us_dir / "data.parquet").exists()
        assert (eu_dir / "data.parquet").exists()

    def test_partitioned_merge_upserts_correctly(self, tmp_path):
        import pandas as pd

        from lakelogic.core.materialization import materialize_dataframe

        contract = DataContract(
            version="1.0",
            primary_key=["id"],
            materialization=Materialization(
                strategy="merge",
                partition_by=["region"],
                target_path=str(tmp_path / "output"),
                format="parquet",
            ),
            server=Server(type="file", path=str(tmp_path / "output"), format="parquet"),
        )

        # First batch
        df1 = pd.DataFrame(
            {
                "id": [1, 2],
                "region": ["US", "US"],
                "value": [10, 20],
            }
        )
        materialize_dataframe(df1, contract)

        # Second batch: update id=1, insert id=3
        df2 = pd.DataFrame(
            {
                "id": [1, 3],
                "region": ["US", "US"],
                "value": [999, 30],
            }
        )
        result = materialize_dataframe(df2, contract)

        # Read back the US partition
        us_file = tmp_path / "output" / "region=US" / "data.parquet"
        merged = pd.read_parquet(us_file)
        assert len(merged) == 3  # id 1 (updated), 2 (unchanged), 3 (new)

        # Check values
        row1 = merged[merged["id"] == 1].iloc[0]
        assert row1["value"] == 999  # Updated

        row2 = merged[merged["id"] == 2].iloc[0]
        assert row2["value"] == 20  # Unchanged

    def test_partitioned_merge_scopes_to_affected_partitions(self, tmp_path):
        import pandas as pd

        from lakelogic.core.materialization import materialize_dataframe

        contract = DataContract(
            version="1.0",
            primary_key=["id"],
            materialization=Materialization(
                strategy="merge",
                partition_by=["region"],
                target_path=str(tmp_path / "output"),
                format="parquet",
            ),
            server=Server(type="file", path=str(tmp_path / "output"), format="parquet"),
        )

        # First batch: both US and EU
        df1 = pd.DataFrame(
            {
                "id": [1, 2],
                "region": ["US", "EU"],
                "value": [10, 20],
            }
        )
        materialize_dataframe(df1, contract)

        # Second batch: only US data
        df2 = pd.DataFrame(
            {
                "id": [1],
                "region": ["US"],
                "value": [999],
            }
        )
        materialize_dataframe(df2, contract)

        # EU partition should be untouched
        eu_data = pd.read_parquet(tmp_path / "output" / "region=EU" / "data.parquet")
        assert len(eu_data) == 1
        assert eu_data.iloc[0]["value"] == 20

        # US partition should be updated
        us_data = pd.read_parquet(tmp_path / "output" / "region=US" / "data.parquet")
        assert len(us_data) == 1
        assert us_data.iloc[0]["value"] == 999

    def test_partitioned_merge_polars_input(self, tmp_path):
        import polars as pl

        from lakelogic.core.materialization import materialize_dataframe

        contract = DataContract(
            version="1.0",
            primary_key=["id"],
            materialization=Materialization(
                strategy="merge",
                partition_by=["category"],
                target_path=str(tmp_path / "output"),
                format="parquet",
            ),
            server=Server(type="file", path=str(tmp_path / "output"), format="parquet"),
        )

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "category": ["A", "B", "A"],
                "amount": [100.0, 200.0, 300.0],
            }
        )

        result = materialize_dataframe(df, contract)
        assert result["rows_written"] == 3


# ── DataProcessor Integration Tests ──────────────────────────────────────────


class TestProcessorBacklogFeatures:
    def test_processor_forget(self, tmp_path):
        import polars as pl
        import yaml

        contract_data = {
            "version": "1.0",
            "info": {"title": "Test", "version": "1.0"},
            "model": {
                "fields": [
                    {"name": "customer_id", "type": "string"},
                    {"name": "email", "type": "string", "pii": True},
                    {"name": "score", "type": "float"},
                ]
            },
        }
        contract_file = tmp_path / "contract.yaml"
        with open(contract_file, "w") as f:
            yaml.dump(contract_data, f)

        from lakelogic.core.processor import DataProcessor

        proc = DataProcessor(str(contract_file), engine="polars")

        df = pl.DataFrame(
            {
                "customer_id": ["c1", "c2"],
                "email": ["a@x.com", "b@x.com"],
                "score": [90.0, 85.0],
            }
        )

        result = proc.forget(df, "customer_id", ["c1"])
        assert result["email"][0] is None  # PII nullified
        assert result["email"][1] == "b@x.com"

    def test_processor_mask_pii(self, tmp_path):
        import polars as pl
        import yaml

        contract_data = {
            "version": "1.0",
            "info": {"title": "Test", "version": "1.0"},
            "model": {
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string", "pii": True},
                ]
            },
        }
        contract_file = tmp_path / "contract.yaml"
        with open(contract_file, "w") as f:
            yaml.dump(contract_data, f)

        from lakelogic.core.processor import DataProcessor

        proc = DataProcessor(str(contract_file), engine="polars")

        df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        result = proc.mask_pii(df, strategy="redact")
        assert result["name"][0] == "***REDACTED***"
        assert result["name"][1] == "***REDACTED***"

    def test_processor_generate_date_dimension(self):
        from lakelogic.core.processor import DataProcessor

        df = DataProcessor.generate_date_dimension(
            start_date="2024-06-01",
            end_date="2024-06-30",
            fiscal_year_start_month=7,
            engine="polars",
        )
        assert df.height == 30
        assert "fiscal_year" in df.columns

        # June with fiscal year starting July → FY2023
        assert df["fiscal_year"][0] == 2023

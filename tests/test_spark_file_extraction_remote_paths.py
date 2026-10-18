"""PDF/image extraction on Spark must read remote files (abfss://, s3://) through Spark,
because pdfplumber/easyocr only open local paths (Fabric run 2026-10-18: ENOENT on abfss)."""
import os

from lakelogic.core.processor import DataProcessor


class _Row(tuple):
    pass


class _FakeSpark:
    def __init__(self, data):
        self.data, self.loaded = data, []

    @property
    def read(self):
        return self

    def format(self, fmt):
        assert fmt == "binaryFile"
        return self

    def load(self, path):
        self.loaded.append(path)
        return self

    def select(self, *_):
        return self

    def head(self):
        return (self.data,)


def test_remote_path_is_read_through_spark_and_original_path_kept():
    seen = {}

    def extract(path, cfg):
        seen["path"] = path
        with open(path, "rb") as f:
            seen["bytes"] = f.read()
        return [{"file_path": path, "name": "x"}]

    remote = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/a.pdf"
    spark = _FakeSpark(bytearray(b"%PDF-1.4"))
    rows = DataProcessor._extract_file_via_spark(spark, remote, None, extract)
    assert spark.loaded == [remote]
    assert seen["bytes"] == b"%PDF-1.4"
    assert seen["path"].endswith(".pdf") and not os.path.exists(seen["path"])
    assert rows == [{"file_path": remote, "name": "x"}]


def test_local_path_is_passed_straight_through():
    def extract(path, cfg):
        return [{"p": path}]

    assert DataProcessor._extract_file_via_spark(None, "/tmp/a.pdf", None, extract) == [{"p": "/tmp/a.pdf"}]

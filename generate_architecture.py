"""
LakeLogic Architecture Diagram Generator
Uses the 'diagrams' Python library to create a professional architecture diagram.
Run: python docs/generate_architecture.py
Output: docs/assets/lakelogic_architecture.png
"""

import os

os.environ["PATH"] += os.pathsep + r"C:\Program Files\Graphviz\bin"

from diagrams import Cluster, Diagram, Edge
from diagrams.generic.database import SQL
from diagrams.generic.storage import Storage
from diagrams.onprem.analytics import Spark
from diagrams.onprem.compute import Server
from diagrams.programming.language import Python

output_dir = os.path.join(os.path.dirname(__file__), "assets")
output_path = os.path.join(output_dir, "lakelogic_architecture")

graph_attr = {
    "bgcolor": "#0d1117",
    "fontcolor": "#e6e6e6",
    "fontsize": "18",
    "pad": "1.0",
    "ranksep": "1.5",
    "nodesep": "0.6",
    "label": "LakeLogic: Data Contracts as Quality Gates",
    "labelloc": "t",
    "fontname": "Helvetica-Bold",
    "splines": "ortho",
}

node_attr = {
    "fontcolor": "#ffffff",
    "fontname": "Helvetica",
    "fontsize": "10",
    "style": "filled,rounded",
    "shape": "box",
}

edge_attr = {
    "color": "#444444",
    "penwidth": "1.5",
}

with Diagram(
    "",
    filename=output_path,
    show=False,
    direction="LR",
    outformat="png",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
):
    # ── Data Sources ──
    with Cluster(
        "Data Sources",
        graph_attr={
            "bgcolor": "#0f1520",
            "fontcolor": "#aaa",
            "fontname": "Helvetica",
            "style": "rounded,dashed",
            "color": "#10529c",
            "fontsize": "12",
        },
    ):
        sources = SQL("Files  APIs\nDatabases\nCloud Storage")

    # ── Contracts ──
    with Cluster(
        "Data Contracts  (Domain / System / Layer)",
        graph_attr={
            "bgcolor": "#0f1520",
            "fontcolor": "#aaa",
            "fontname": "Helvetica",
            "style": "rounded,dashed",
            "color": "#4e3e91",
            "fontsize": "12",
            "label": "Data Contracts\nDomain / System / Layer\nLLM Discoverable   Data Mesh",
        },
    ):
        with Cluster(
            "CRM / Salesforce",
            graph_attr={
                "bgcolor": "#111827",
                "fontcolor": "#999",
                "fontname": "Helvetica",
                "style": "rounded",
                "color": "#333",
                "fontsize": "10",
            },
        ):
            crm = Storage("bronze.yaml\nsilver.yaml\ngold.yaml")

        with Cluster(
            "Marketing / GA",
            graph_attr={
                "bgcolor": "#111827",
                "fontcolor": "#999",
                "fontname": "Helvetica",
                "style": "rounded",
                "color": "#333",
                "fontsize": "10",
            },
        ):
            mkt = Storage("bronze.yaml\nsilver.yaml\ngold.yaml")

    # ── LakeLogic OSS ──
    with Cluster(
        "LakeLogic OSS",
        graph_attr={
            "bgcolor": "#1a1035",
            "fontcolor": "#ccc",
            "fontname": "Helvetica-Bold",
            "style": "filled,rounded",
            "color": "#4e3e91",
            "fontsize": "14",
            "penwidth": "3",
        },
    ):
        lakelogic = Python("LakeLogic\nGatekeeper")
        spark_eng = Spark("Spark")
        polars_eng = Server("Polars")
        duckdb_eng = SQL("DuckDB")

    # ── Lakehouse ──
    with Cluster(
        "Lakehouse (Medallion)",
        graph_attr={
            "bgcolor": "#0f1520",
            "fontcolor": "#aaa",
            "fontname": "Helvetica",
            "style": "rounded,dashed",
            "color": "#006151",
            "fontsize": "12",
        },
    ):
        bronze = Storage("BRONZE\nRaw Capture")
        silver = Storage("SILVER\nValidated")
        gold = Storage("GOLD\nAggregates")
        quarantine = SQL("QUARANTINE\nBad Data")

    # ── External Logic ──
    with Cluster(
        "External Logic (Gold)",
        graph_attr={
            "bgcolor": "#0f1520",
            "fontcolor": "#aaa",
            "fontname": "Helvetica",
            "style": "rounded,dashed",
            "color": "#8c6512",
            "fontsize": "12",
            "label": "External Logic (Gold)\n20% Custom Code",
        },
    ):
        scripts = Python("Notebooks\nPython Scripts")

    # ── Flow ──
    sources >> Edge(color="#10529c") >> lakelogic
    crm >> Edge(color="#4e3e91", style="dashed") >> lakelogic
    mkt >> Edge(color="#4e3e91", style="dashed") >> lakelogic

    lakelogic >> Edge(color="#8d3d23") >> bronze
    lakelogic >> Edge(color="#006151") >> silver
    lakelogic >> Edge(color="#8c6512") >> gold
    silver >> Edge(color="#8c2020", style="dashed") >> quarantine

    lakelogic >> Edge(color="#4e3e91", style="dashed") >> scripts
    scripts >> Edge(color="#8c6512") >> gold

print("Diagram generated: " + output_path + ".png")

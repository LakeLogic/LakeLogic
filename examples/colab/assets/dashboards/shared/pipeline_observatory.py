import os

import panel as pn
import polars as pl

pn.extension("tabulator")


def create_observatory_view():
    log_base = r"C:\_Personal\_SaaS\lakelogic-ra-rideflow\lakehouse"

    # Try reading the actual delta logs from the platform
    all_logs = []

    # Attempt to pull logs from all domains
    if os.path.exists(log_base):
        for domain in ["marketplace"]:
            dpath = os.path.join(log_base, domain, "_logs")
            if os.path.exists(dpath) and os.path.exists(os.path.join(dpath, "_delta_log")):
                try:
                    df = pl.read_delta(dpath)
                    all_logs.append(df)
                except Exception as e:
                    print(f"Error reading {domain} log: {e}")

    if all_logs:
        df_logs = pl.concat(all_logs)

        # Calculate summary metrics per domain
        # Example metrics: total_runs, total_duration, cost_usd, rows_quarantined

        df_summary = df_logs.group_by("domain", "system", "layer").agg(
            [
                pl.col("run_id").count().alias("runs"),
                pl.col("target_records").sum().alias("total_records"),
                pl.col("quarantined_records").sum().alias("quarantine_hits"),
                pl.col("duration_ms").mean().alias("avg_duration_ms"),
                pl.col("cost_usd").sum().alias("total_cost_usd"),
            ]
        )

        # Turn to pandas for hvplot
        pdf = df_summary.to_pandas()

        # Convert the raw logs for the datatable
        pdf_logs = (
            df_logs.select(
                [
                    "started_at",
                    "domain",
                    "system",
                    "entity",
                    "layer",
                    "status",
                    "target_records",
                    "quarantined_records",
                    "cost_usd",
                ]
            )
            .sort("started_at", descending=True)
            .head(50)
            .to_pandas()
        )

        # KPI Bar Chart
        bar_chart = pdf.hvplot.bar(
            x="domain",
            y="quarantine_hits",
            by="layer",
            stacked=True,
            title="Quarantine Hits by Layer per Domain",
            ylabel="Quarantined Records",
            height=300,
        )

        datatable = pn.widgets.Tabulator(
            pdf_logs, layout="fit_data_stretch", pagination="remote", page_size=15, theme="fast"
        )

        body = pn.Column(
            pn.pane.Markdown("### 🔍 Live Pipeline SLA Diagnostics"),
            bar_chart,
            pn.pane.Markdown("#### Recent Execution Logs", margin=(20, 0, 0, 0)),
            datatable,
            sizing_mode="stretch_both",
        )

        return body
    else:
        return pn.pane.Markdown("No Delta run logs exist in `lakehouse/_logs/`. Please run the pipeline first!")

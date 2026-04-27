"""
RideFlow Streaming Dashboard — Panel (HoloViz) real-time dashboard.

Reads from local lakehouse Delta tables and provides a live-updating
dashboard with trip volume, revenue, cancellations, surge, and pipeline
health panels.

Works in:
- Jupyter / VS Code notebooks (inline)
- Google Colab (via pn.extension(comms='colab'))
- Standalone Panel server (panel serve)
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def _safe_read_delta(path: str) -> Optional[pd.DataFrame]:
    """Attempt to read a Delta table, returning None on failure."""
    try:
        import polars as pl

        return pl.read_delta(path).to_pandas()
    except Exception:
        pass
    try:
        from deltalake import DeltaTable

        return DeltaTable(path).to_pandas()
    except Exception:
        pass
    return None


def create_streaming_dashboard_inline(
    lakehouse_root: str = "./lakehouse",
    **kwargs,
):
    """
    Create a highly-analytical streaming dashboard for the notebook inline view.
    Features a spacious, card-based CSS grid layout restricted to 5 core visuals.
    """
    import holoviews as hv
    import hvplot.pandas  # noqa: F401
    import panel as pn

    # Only initialise Panel/HoloViews if the caller hasn't done so already.
    # Calling pn.extension() multiple times in a notebook injects duplicate
    # Bokeh JS bundles and corrupts the widget renderer → blank charts.
    if not getattr(pn.state, "_extensions_loaded", False):
        pn.extension("tabulator", sizing_mode="stretch_width")
        hv.extension("bokeh", logo=False)
        pn.state._extensions_loaded = True

    domain = kwargs.get("domain", "marketplace")
    system = kwargs.get("system", "rideflow")
    refresh_ms = kwargs.get("refresh_ms", 3000)

    base = Path(lakehouse_root) / domain / "silver"
    trips_path = str(base / f"silver_{system}_trips")
    drivers_path = str(base / f"silver_{system}_driver_profiles")

    # ── Styling Config ───────────────────────────────────────────────
    CARD_STYLE = {
        "background-color": "#21212b",
        "border": "1px solid #333",
        "border-radius": "8px",
        "box-shadow": "0 4px 6px rgba(0,0,0,0.3)",
        "padding": "10px",
    }

    PRIMARY_COLOR = "#fdfd96"  # Pastel yellow from reference
    SECONDARY_COLOR = "#e6e681"  # Slightly darker yellow

    # ── Interactive Control Widgets ──────────────────────────────────
    time_agg_selector = pn.widgets.Select(
        name="Aggregation Period", options=["Hour", "Day", "Week", "Month"], value="Hour", width=200
    )

    # ── Reactive Panes ───────────────────────────────────────────────
    status = pn.pane.Markdown("**⏳ Waiting for data...**", styles={"color": "#888"})
    kpi_md = pn.pane.Markdown("", styles={"white-space": "nowrap", "min-width": "max-content"})

    # EXACTLY 5 VISUALS
    chart_trips = pn.Column(sizing_mode="stretch_both", min_height=260)
    chart_top_drivers = pn.Column(sizing_mode="stretch_both", min_height=260)
    chart_city = pn.Column(sizing_mode="stretch_both", min_height=260)
    chart_ratings = pn.Column(sizing_mode="stretch_both", min_height=260)
    chart_surge = pn.Column(sizing_mode="stretch_both", min_height=260)

    def get_time_period(dt_series: pd.Series, agg_level: str) -> pd.Series:
        """Extract the temporal bucket from a datetime series."""
        if agg_level == "Hour":
            return dt_series.dt.strftime("%Y-%m-%d %H:00")
        elif agg_level == "Day":
            return dt_series.dt.strftime("%Y-%m-%d")
        elif agg_level == "Week":
            return dt_series.dt.to_period("W").dt.start_time.strftime("%Y-%m-%d")
        elif agg_level == "Month":
            return dt_series.dt.strftime("%Y-%m")
        return dt_series.dt.strftime("%Y-%m-%d")

    @pn.depends(time_agg_selector.param.value, watch=True)
    def refresh(agg_level="Hour"):
        if hasattr(agg_level, "new"):
            agg_level = agg_level.new

        df_trips = _safe_read_delta(trips_path)
        df_drivers = _safe_read_delta(drivers_path)

        if df_trips is None or len(df_trips) == 0:
            status.object = "**⏳ No data yet** — run the pipeline cells above"
            return

        # Numeric casting
        for col in ["fare_amount", "surge_multiplier", "tip_amount", "distance_km", "driver_rating"]:
            if col in df_trips.columns:
                df_trips[col] = pd.to_numeric(df_trips[col], errors="coerce")

        if "requested_at" in df_trips.columns:
            df_trips["requested_at"] = pd.to_datetime(df_trips["requested_at"], errors="coerce", utc=True)
            df_trips["period"] = get_time_period(df_trips["requested_at"], time_agg_selector.value)
        else:
            df_trips["period"] = "Unknown"

        # Apply multi-table join for top drivers
        if df_drivers is not None and not df_drivers.empty and "driver_id" in df_trips.columns:
            df = df_trips.merge(df_drivers[["driver_id", "name"]], on="driver_id", how="left")
            df["driver_name"] = df["name"].fillna("Unknown Driver")
        else:
            df = df_trips
            df["driver_name"] = df.get("driver_id", "Unknown ID")

        # ── KPI Header ───────────────────────────────────────────────
        total = len(df)
        rev = df["fare_amount"].sum() if "fare_amount" in df.columns else 0
        avg_dist = df["distance_km"].mean() if "distance_km" in df.columns else 0
        avg_rating = df["driver_rating"].mean() if "driver_rating" in df.columns else 0

        kpi_md.object = (
            f"### 🚖 **{total:,}** total trips &nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp; "
            f"💰 **£{rev:,.0f}** total revenue &nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp; "
            f"📍 **{avg_dist:.1f}km** avg distance &nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp; "
            f"⭐ **{avg_rating:.2f}** avg driver rating"
        )

        # ── 5 Core Charts (Pastel aesthetic) ─────────────────────────
        try:
            # 1. Trips by Temporal Period
            periodic_trips = df.groupby("period").size().reset_index(name="trips").sort_values("period")
            n_periods = len(periodic_trips)
            tick_step = max(1, n_periods // 12)
            xticks = list(range(0, n_periods, tick_step))
            chart_trips.objects = [
                periodic_trips.hvplot.bar(
                    x="period",
                    y="trips",
                    color=PRIMARY_COLOR,
                    hover_color=SECONDARY_COLOR,
                    height=260,
                    rot=45,
                    responsive=True,
                    toolbar=None,
                    xlabel="Period",
                    ylabel="Trips",
                    xticks=xticks,
                )
            ]
        except Exception as e:
            chart_trips.objects = [pn.pane.Markdown(f"⚠️ Chart error: {e}", styles={"color": "#f87171"})]

        try:
            # 2. Top 10 Drivers by Completed Trips & Revenue
            top_drivers = (
                df.groupby("driver_name")
                .agg(total_revenue=("fare_amount", "sum"), trips=("driver_name", "count"))
                .nlargest(10, "total_revenue")
                .reset_index()
                .sort_values("total_revenue", ascending=True)
            )

            chart_top_drivers.objects = [
                top_drivers.hvplot.barh(
                    x="driver_name",
                    y="total_revenue",
                    hover_cols=["trips"],
                    color=PRIMARY_COLOR,
                    hover_color=SECONDARY_COLOR,
                    height=260,
                    responsive=True,
                    toolbar=None,
                    xlabel="Total Revenue (£)",
                    ylabel="",
                )
            ]
        except Exception as e:
            chart_top_drivers.objects = [pn.pane.Markdown(f"⚠️ Chart error: {e}", styles={"color": "#f87171"})]

        try:
            # 3. Revenue by City (Narrow)
            if "city_code" in df.columns and "fare_amount" in df.columns:
                city_rev = (
                    df.groupby("city_code")["fare_amount"].sum().sort_values(ascending=False).reset_index().head(7)
                )
                city_rev.columns = ["city", "revenue"]
                chart_city.objects = [
                    city_rev.hvplot.barh(
                        x="city",
                        y="revenue",
                        color=PRIMARY_COLOR,
                        hover_color=SECONDARY_COLOR,
                        height=240,
                        responsive=True,
                        toolbar=None,
                        xlabel="Revenue (£)",
                        ylabel="",
                    )
                ]
        except Exception as e:
            chart_city.objects = [pn.pane.Markdown(f"⚠️ Chart error: {e}", styles={"color": "#f87171"})]

        try:
            # 4. Rating Distribution (Narrow)
            if "driver_rating" in df.columns:
                chart_ratings.objects = [
                    df.hvplot.hist(
                        "driver_rating",
                        color=PRIMARY_COLOR,
                        hover_color=SECONDARY_COLOR,
                        bins=10,
                        height=240,
                        responsive=True,
                        toolbar=None,
                        xlabel="Driver Rating",
                        ylabel="Count",
                    )
                ]
        except Exception as e:
            chart_ratings.objects = [pn.pane.Markdown(f"⚠️ Chart error: {e}", styles={"color": "#f87171"})]

        try:
            # 5. Surge Distribution (Narrow)
            if "surge_multiplier" in df.columns:
                chart_surge.objects = [
                    df.hvplot.hist(
                        "surge_multiplier",
                        color=PRIMARY_COLOR,
                        hover_color=SECONDARY_COLOR,
                        bins=15,
                        height=240,
                        responsive=True,
                        toolbar=None,
                        xlabel="Surge Multiplier",
                        ylabel="Count",
                    )
                ]
        except Exception as e:
            chart_surge.objects = [pn.pane.Markdown(f"⚠️ Chart error: {e}", styles={"color": "#f87171"})]

        status.object = f"**✅ Live** — Last updated {pd.Timestamp.now().strftime('%H:%M:%S')}"

    # Setup the periodic callback wrapped properly
    def periodic_loop():
        refresh()

    periodic_loop()
    pn.state.add_periodic_callback(periodic_loop, period=refresh_ms)

    # ── Dashboard Layout (Spacious Grid) ─────────────────────────

    header = pn.Row(
        pn.pane.Markdown(
            "# 🚓 RideFlow Observatory",
            styles={"margin-top": "15px", "white-space": "nowrap", "min-width": "max-content"},
        ),
        pn.layout.HSpacer(),
        pn.Row(time_agg_selector, status, align="center"),
        sizing_mode="stretch_width",
        align="center",
    )

    # Wrap the 5 charts in styled cards
    card_trips = pn.Card(
        chart_trips,
        title="📈 Trip Volume Timeline",
        styles=CARD_STYLE,
        hide_header=False,
        sizing_mode="stretch_both",
        margin=10,
    )
    card_top_drivers = pn.Card(
        chart_top_drivers,
        title="⭐ Top 10 Drivers",
        styles=CARD_STYLE,
        hide_header=False,
        sizing_mode="stretch_both",
        margin=10,
    )

    card_city = pn.Card(
        chart_city,
        title="🏙️ Revenue by City",
        styles=CARD_STYLE,
        hide_header=False,
        sizing_mode="stretch_both",
        margin=10,
    )
    card_ratings = pn.Card(
        chart_ratings,
        title="👍 Driver Quality Distribution",
        styles=CARD_STYLE,
        hide_header=False,
        sizing_mode="stretch_both",
        margin=10,
    )
    card_surge = pn.Card(
        chart_surge,
        title="⚡ Surge Multiplier Profile",
        styles=CARD_STYLE,
        hide_header=False,
        sizing_mode="stretch_both",
        margin=10,
    )

    # Top Row: Wide elements
    top_grid = pn.Row(card_trips, card_top_drivers, sizing_mode="stretch_width", height=350)

    # Bottom Row: 3 Narrow elements
    bottom_grid = pn.Row(card_city, card_ratings, card_surge, sizing_mode="stretch_width", height=320)

    # Master Layout wrapping everything in deep dark background
    master_layout = pn.Column(
        header,
        kpi_md,
        pn.layout.Divider(),
        top_grid,
        bottom_grid,
        sizing_mode="stretch_width",
        styles={"background-color": "#121212", "padding": "20px", "border-radius": "10px"},
    )

    return master_layout


# Map both functions to the inline version for backward compatibility
create_streaming_dashboard = create_streaming_dashboard_inline

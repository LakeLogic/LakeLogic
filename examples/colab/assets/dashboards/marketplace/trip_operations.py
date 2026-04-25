import panel as pn
import polars as pl
import hvplot.pandas
import os

pn.extension('tabulator')

def create_trip_operations_view():
    trips_path = r"C:\_Personal\_SaaS\lakelogic-ra-rideflow\lakehouse\marketplace\silver\silver_rideflow_trips"
    
    if os.path.exists(trips_path):
        try:
            df_trips = pl.read_delta(trips_path)
            
            # Show a subset of trips
            df_display = df_trips.head(100).to_pandas()
            
            # Simple aggregation by driver for chart
            df_driver_counts = (
                df_trips
                .group_by("driver_id")
                .agg(pl.count("trip_id").alias("trip_count"))
                .sort("trip_count", descending=True)
                .head(15)
                .to_pandas()
            )
            
            bar_chart = df_driver_counts.hvplot.bar(
                x="driver_id", 
                y="trip_count",
                title="Top 15 Drivers by Completed Trips",
                rot=45,
                height=300
            )
            
            datatable = pn.widgets.Tabulator(
                df_display, 
                layout='fit_data_stretch',
                pagination='remote', 
                page_size=12,
                theme='fast'
            )
            
            body = pn.Column(
                pn.pane.Markdown("### 🚗 Marketplace Pulse: Silver Trips Component"),
                bar_chart,
                pn.pane.Markdown("#### PII Masked Silver Trip Sample Feed", margin=(20, 0, 0, 0)),
                datatable,
                sizing_mode="stretch_both"
            )
            return body
            
        except Exception as e:
            return pn.pane.Markdown(f"### Error reading trips delta lake: {e}")
            
    return pn.pane.Markdown("### `marketplace.silver_rideflow_trips` table has not been generated.")

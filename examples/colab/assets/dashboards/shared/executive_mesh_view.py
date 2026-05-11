import panel as pn
import polars as pl


# Create mock data or load real delta data for Executive View
def create_executive_view():
    # Premium styled KPI Cards using custom HTML
    card_style = """
    <div style="background-color: #1e1e1e; border-radius: 8px; padding: 20px;
         box-shadow: 0 4px 6px rgba(0,0,0,0.3); text-align: center;
         border-top: 4px solid {color};">
        <div style="color: #a0a0a0; font-size: 14px; text-transform: uppercase;
             letter-spacing: 1px; margin-bottom: 10px;">{title}</div>
        <div style="color: #ffffff; font-size: 32px; font-weight: bold;
             font-family: 'Inter', sans-serif;">{value}</div>
    </div>
    """

    kpi_cards = pn.Row(
        pn.pane.HTML(
            card_style.format(title="GMV Today", value="2.4M £", color="#2ecc71"), sizing_mode="stretch_width"
        ),
        pn.pane.HTML(
            card_style.format(title="Active Riders", value="48,200", color="#3498db"), sizing_mode="stretch_width"
        ),
        pn.pane.HTML(
            card_style.format(title="Active Drivers", value="12,100", color="#e67e22"), sizing_mode="stretch_width"
        ),
        pn.pane.HTML(card_style.format(title="Take Rate", value="28.5%", color="#9b59b6"), sizing_mode="stretch_width"),
        sizing_mode="stretch_width",
        margin=(0, 0, 30, 0),
    )

    # Domain Health Matrix
    health_data = [
        {
            "Pipeline Area": "Trips",
            "Status": "✅ GREEN",
            "Freshness": "< 2 min",
            "Quality": "99.8%",
            "Quarantine": "0.2%",
        },
        {
            "Pipeline Area": "Riders",
            "Status": "✅ GREEN",
            "Freshness": "< 5 min",
            "Quality": "99.1%",
            "Quarantine": "0.9%",
        },
        {
            "Pipeline Area": "Drivers",
            "Status": "✅ GREEN",
            "Freshness": "< 5 min",
            "Quality": "98.5%",
            "Quarantine": "1.5%",
        },
        {
            "Pipeline Area": "Telemetry",
            "Status": "⚠️ AMBER",
            "Freshness": "22 min",
            "Quality": "94.2%",
            "Quarantine": "5.8%",
        },
    ]

    df_health = pl.DataFrame(health_data).to_pandas()

    health_title = pn.pane.Markdown("### 🏢 Domain Health Matrix", margin=(20, 0, 0, 0))
    health_table = pn.widgets.Tabulator(
        df_health, layout="fit_data_stretch", theme="fast", show_index=False, disabled=True
    )

    # Cost & Pipeline Stats
    cost_data = [
        {"Platform": "Google Ads (CAC)", "Cost": "£8.40"},
        {"Platform": "Meta Ads (CAC)", "Cost": "£14.20"},
        {"Platform": "Pipeline Compute (Per Run)", "Cost": "£4.12"},
    ]
    df_cost = pl.DataFrame(cost_data).to_pandas()

    cost_table = pn.widgets.Tabulator(df_cost, layout="fit_data_stretch", theme="fast", show_index=False, disabled=True)

    body = pn.Column(
        kpi_cards,
        pn.Row(
            pn.Column(health_title, health_table, sizing_mode="stretch_width"),
            pn.Column(
                pn.pane.Markdown("### 💰 Financials & Compute", margin=(20, 0, 0, 0)),
                cost_table,
                sizing_mode="stretch_width",
            ),
        ),
        sizing_mode="stretch_both",
    )

    return body

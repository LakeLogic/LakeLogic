import glob
import os

import panel as pn
import polars as pl
import yaml

pn.extension("tabulator")


def load_ai_act_contracts(base_path):
    registry_data = []

    # Search for all contracts across domains
    search_pattern = os.path.join(base_path, "**", "*.yaml")
    contract_files = glob.glob(search_pattern, recursive=True)

    for cfile in contract_files:
        try:
            with open(cfile, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

            # Check if this contract has EU AI Act metadata
            compliance = data.get("compliance", {})
            ai_act = compliance.get("eu_ai_act", {})

            if ai_act and ai_act.get("applicable", False):
                registry_data.append(
                    {
                        "Model Domain": data.get("domain", "Unknown"),
                        "System": data.get("system", "Unknown"),
                        "Entity": data.get("dataset", "Unknown"),
                        "Risk Tier": str(ai_act.get("risk_tier", "Unknown")).upper(),
                        "Purpose": ai_act.get("ai_system_purpose", ""),
                        "Bias Examination": "✅" if ai_act.get("bias_examination", False) else "❌",
                        "Transparency": "✅" if ai_act.get("transparency_disclosure", False) else "❌",
                        "Human Oversight": "✅" if ai_act.get("human_oversight", False) else "❌",
                        "Logging": "✅" if ai_act.get("logging_enabled", False) else "❌",
                    }
                )
        except Exception:
            pass

    return registry_data


def create_ai_registry_view():
    contract_base = r"C:\_Personal\_SaaS\lakelogic-ra-rideflow\domains_rideflow"
    registry_data = load_ai_act_contracts(contract_base)

    if not registry_data:
        return pn.pane.Markdown("### No AI Act compliance definitions found in the contracts.")

    df = pl.DataFrame(registry_data).to_pandas()

    # Render table
    datatable = pn.widgets.Tabulator(df, layout="fit_data_stretch", theme="fast", show_index=False)

    # Count metrics
    high_risk = sum(1 for d in registry_data if d["Risk Tier"] == "HIGH")
    limited_risk = sum(1 for d in registry_data if d["Risk Tier"] == "LIMITED")

    kpi_cards = pn.Row(
        pn.indicators.Number(name="Total ML Models", value=len(registry_data), format="{value}", colors=[(1, "blue")]),
        pn.indicators.Number(
            name="High Risk Systems",
            value=high_risk,
            format="{value}",
            colors=[(1, "red")] if high_risk > 0 else [(1, "green")],
        ),
        pn.indicators.Number(name="Limited Risk Systems", value=limited_risk, format="{value}", colors=[(1, "orange")]),
        sizing_mode="stretch_width",
    )

    body = pn.Column(
        pn.pane.Markdown("### ⚖️ EU AI Act Compliance Registry (Regulation 2024/1689)"),
        pn.pane.Markdown(
            "This live registry auto-discovers all LakeLogic data contracts "
            "globally tagged with `eu_ai_act` metadata. It tracks bias-examination, "
            "human-oversight tooling, and risk categorization across the mesh."
        ),
        kpi_cards,
        pn.pane.Markdown("#### Registered AI/ML Pipeline Contracts", margin=(20, 0, 0, 0)),
        datatable,
        sizing_mode="stretch_both",
    )

    return body

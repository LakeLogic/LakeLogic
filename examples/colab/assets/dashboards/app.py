import panel as pn
import os

# Import dashboard modules
try:
    from shared.executive_mesh_view import create_executive_view
    from shared.pipeline_observatory import create_observatory_view
    from marketplace.trip_operations import create_trip_operations_view
    from compliance.ai_act_registry import create_ai_registry_view
except ImportError:
    # Handle direct execution from within dashboards dir
    import sys
    sys.path.append(os.path.dirname(__file__))
    from shared.executive_mesh_view import create_executive_view
    from shared.pipeline_observatory import create_observatory_view
    from marketplace.trip_operations import create_trip_operations_view
    from compliance.ai_act_registry import create_ai_registry_view

pn.extension('tabulator', design='bootstrap', template='fast')

def create_app():
    # Create the views
    tab_executive = create_executive_view()
    tab_observatory = create_observatory_view()
    tab_trips = create_trip_operations_view()
    tab_compliance = create_ai_registry_view()
    
    # Create main layout with Tabs
    tabs = pn.Tabs(
        ("🚀 Executive Mesh", tab_executive),
        ("⚙️ Marketplace Pulse", tab_trips),
        ("🛡️ Pipeline Observatory", tab_observatory),
        ("⚖️ EU AI Act Registry", tab_compliance),
        dynamic=True,
        sizing_mode="stretch_both"
    )

    template = pn.template.FastListTemplate(
        title="RideFlow Data Mesh",
        sidebar=[
            pn.pane.Markdown("### Navigation"),
            pn.pane.Markdown("Use the tabs to switch between Domain logic, Executive readouts, and Pipeline SLA observability."),
            pn.pane.Markdown("---"),
            pn.pane.Markdown("**Active Data Mesh Domains:**"),
            pn.pane.Markdown("- Marketplace"),
            pn.pane.Markdown("- Payments"),
            pn.pane.Markdown("- Operations"),
            pn.pane.Markdown("- Marketing")
        ],
        main=[tabs],
        header_background="#000000",
        accent_base_color="#1f77b4"
    )
    
    return template

if __name__.startswith("bokeh"):
    # This is run via `panel serve app.py`
    app = create_app()
    app.servable()
elif __name__ == "__main__":
    app = create_app()
    app.show(port=5006)

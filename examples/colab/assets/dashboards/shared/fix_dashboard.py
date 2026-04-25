import re

path = r'C:\_Personal\_SaaS\lakelogic\examples\colab\assets\dashboards\shared\streaming_dashboard.py'
with open(path, 'r', encoding='utf-8') as f:
    code = f.read()

# 1. Update kpi_md initialization
code = code.replace(
    'kpi_md = pn.pane.Markdown("")',
    'kpi_md = pn.pane.Markdown("", styles={"white-space": "nowrap", "min-width": "max-content"})'
)

# 2. Update chart_* initialization from pn.pane.HoloViews to pn.Column
for name in ["chart_trips", "chart_top_drivers", "chart_city", "chart_ratings", "chart_surge"]:
    code = re.sub(
        rf'{name} = pn\.pane\.HoloViews\(.*?\)',
        f'{name} = pn.Column(sizing_mode="stretch_both")',
        code
    )

# 3. Fix KPI text (remove the <div>)
code = code.replace("<div style='white-space: nowrap;'>", "")
code = code.replace("</div>", "")

# 4. Update chart_*.object = ... to chart_*.objects = [...]
def replacer(match):
    return match.group(0).replace('.object =', '.objects = [') + ']'

code = re.sub(r'chart_city\.object = [\s\S]*?xlabel="Revenue \([^)]+\)", ylabel=""\n\s*\)', replacer, code)
code = re.sub(r'chart_ratings\.object = [\s\S]*?xlabel="Driver Rating", ylabel="Count"\n\s*\)', replacer, code)
code = re.sub(r'chart_surge\.object = [\s\S]*?xlabel="Surge Multiplier", ylabel="Count"\n\s*\)', replacer, code)
code = re.sub(r'chart_trips\.object = [\s\S]*?xlabel="Period", ylabel="Trips", xticks=xticks\n\s*\)', replacer, code)
code = re.sub(r'chart_top_drivers\.object = [\s\S]*?xlabel="Total Revenue \([^)]+\)", ylabel=""\n\s*\)', replacer, code)

with open(path, 'w', encoding='utf-8') as f:
    f.write(code)

print('Done!')

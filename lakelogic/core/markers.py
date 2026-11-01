"""Row markers LakeLogic carries through every layer.

``unknown_fields: drop`` keeps only a contract's declared columns. A marker column survives it
anyway and is never reported as schema drift, so what it marks stays visible downstream.
"""

#: ``_synthetic=true`` marks a row the synthetic seed generated, not one read from a source.
CARRIED_MARKERS = ("_synthetic",)

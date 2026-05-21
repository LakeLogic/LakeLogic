"""
LakeLogic Knowledge Base — error pattern → remediation mapping.

OSS ships with an empty store. The Cloud tier populates it with curated
remediation docs, known failure patterns, and Zeus-generated fix suggestions.

Usage:
    from lakelogic.knowledge import KnowledgeBase
    kb = KnowledgeBase()
    hint = kb.lookup("null_violation", contract_name="orders")
"""

from lakelogic.knowledge.store import KnowledgeBase, KnowledgeEntry

__all__ = ["KnowledgeBase", "KnowledgeEntry"]

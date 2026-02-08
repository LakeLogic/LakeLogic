# Getting Started

Your first LakeLogic experience. Start here.

## Examples

### [basic_validation/](basic_validation/)
**Time**: 5 minutes

The simplest possible LakeLogic contract. Learn:
- Schema definition with field types
- Basic quality rules (email format, positive age)
- Pre/post transformations
- Quarantine output with error reasons

**Run it**:
```bash
cd basic_validation
lakelogic run --contract contract.yaml --source data/sample_customers.csv
```

---

## Next Steps

After completing this, move to:
- [02_tutorials/medallion_architecture/](../02_tutorials/medallion_architecture/) - Learn Bronze → Silver pipelines

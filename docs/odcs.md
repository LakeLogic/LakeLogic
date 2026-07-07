# Open Data Contract Standard (ODCS)

LakeLogic provides native support for the [Open Data Contract Standard (ODCS)](https://github.com/bitol-io/open-data-contract-standard), allowing you to bring existing standard contracts into your data platform and execute them immediately.

## How It Works

When LakeLogic reads a dictionary or YAML file, it automatically detects the ODCS fingerprint (specifically the `kind: DataContract` and `apiVersion` fields). When detected, LakeLogic's internal parser intercepts the payload and translates it transparently into an executable LakeLogic pipeline.

This means you can use standard Python objects like `DataContract(**my_odcs_dict)` or the CLI `lakelogic run my_odcs_contract.yaml` exactly as you normally would.

### What gets translated?

1. **Version:** `apiVersion` is mapped to LakeLogic's `version`.
2. **Dataset:** Supported `dataset` names map directly into LakeLogic's `info.title`.
3. **Schema Mapping:** The ODCS `schema` array is mapped over to the `model.fields` array that LakeLogic uses for row-by-row enforcement. Parameter translations include:
   - ODCS `name` → LakeLogic `name`
   - ODCS `type` → LakeLogic SQL `type`
   - ODCS `required` (boolean) → LakeLogic `required` checks
   - ODCS `pii` (boolean) → LakeLogic PII obfuscation routines
   - ODCS `description` → LakeLogic table/column comments

### The `customProperties` Extension

ODCS is primarily a *declarative data definition* standard, meaning it typically doesn't tell a data engine *how* to load the data (e.g. `merge` vs `append`, `parquet` vs `delta`, `s3://...` vs Azure). 

To give LakeLogic the execution context it needs to run your pipeline, you simply place your LakeLogic connection and materialization instructions into the official ODCS extension block: `customProperties.lakelogic`.

---

## Example ODCS Contract

The following is a fully compliant Open Data Contract Standard YAML file that is 100% executable by LakeLogic:

```yaml
kind: DataContract
apiVersion: v3.1.0
dataset: customers
schema:
  - name: id
    type: integer
    required: true
    description: Primary customer ID
  - name: email
    type: string
    pii: true
    required: true

# LakeLogic execution instructions
customProperties:
  lakelogic:
    tier: silver
    source:
      type: file
      path: s3://landing/customers/
      format: parquet
    materialization:
      strategy: merge
      primary_key: [id]
      target_path: silver.customers
    quality:
      row_rules:
        - sql: "email LIKE '%@%.%'"
```

When you pass this file to LakeLogic, it will pull the schema out of the main ODCS definition, grab the `tier`, `source`, `materialization`, and `quality` configuration out of `customProperties.lakelogic`, and execute the pipeline!

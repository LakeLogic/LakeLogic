## Standing rules for this repository (LakeLogic)

This repository governs its data with LakeLogic contracts (Open Lakehouse Contracts). When you
add or change data, follow these rules without being asked:

1. **A new table gets a contract.** Before writing pipeline code for a new table, write its
   contract (`olc guide`, then `olc reference <Section>`; `lakelogic bootstrap` drafts them from
   a landing zone) and check it with `lakelogic validate --contract <file>`.
2. **A schema change starts in the contract.** Change the contract first, then the code. Before
   renaming or retyping a column, find what reads the table (contracts whose source or upstream
   names it) and say what the change affects.
3. **A new quality rule is tested before it ships.** Generate rows that deliberately break the
   rules, run the contract on them, and confirm the rule caught its rows:
   `lakelogic generate --contract <file> --rows 500 --invalid-ratio 0.1 --output test.parquet`,
   then `lakelogic run`, then `lakelogic assert --report <report.json> --expect-all-invalid-quarantined`.
   If a rule catches nothing, the rule is wrong: fix it before committing.
4. **Validate before you commit.** Do not commit a contract that `lakelogic validate` rejects.
5. **Never edit generated output by hand.** Change the contract and regenerate.
6. **Business rules are the data owner's decision.** Propose allowed values, ranges and
   thresholds; do not invent them from a sample. Mark them as proposals for the owner to confirm.

Full guide (install, run, outputs, every command): `lakelogic agent-guide`.

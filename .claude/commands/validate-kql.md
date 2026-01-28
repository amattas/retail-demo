---
description: Validate KQL scripts for common issues
---
```bash
echo "=== KQL Files ===" && \
ls -la /home/user/retail-demo/fabric/kql_database/*.kql 2>/dev/null && \
echo -e "\n=== Checking for script wrappers ===" && \
grep -l "\.execute database script" /home/user/retail-demo/fabric/kql_database/*.kql 2>/dev/null | wc -l && \
echo " files use .execute database script wrapper" && \
echo -e "\n=== Table naming (should be snake_case) ===" && \
grep -oh "\.create-or-alter table [a-zA-Z_]*" /home/user/retail-demo/fabric/kql_database/*.kql 2>/dev/null | sort -u | head -20
```

Review KQL files for:

1. **Script wrapper**: All scripts should use `.execute database script <| ... ` for batch execution
2. **Table naming**: Event tables should use snake_case (e.g., `receipt_created`, not `ReceiptCreated`)
3. **Materialized view naming**: Should be unique and descriptive
4. **Column consistency**: Check that column names match schema definitions in `datagen/src/retail_datagen/streaming/schemas.py`

Report any inconsistencies found.

# Scripts

Utility scripts for configuring and deploying the retail demo components.

## configure_semantic_model.py

Configures the Power BI semantic model to connect to your Fabric lakehouse.

### Purpose

When deploying the Power BI semantic model to a new environment, the lakehouse connection string must be updated to point to your specific workspace and lakehouse. This script automates that process by modifying the `expressions.tmdl` file with the correct OneLake connection URL.

### Usage

#### Option 1: Command-line Arguments

```bash
python scripts/configure_semantic_model.py \
    --workspace-id <your-workspace-guid> \
    --lakehouse-id <your-lakehouse-guid>
```

#### Option 2: Environment Variables

```bash
export WORKSPACE_ID=<your-workspace-guid>
export LAKEHOUSE_ID=<your-lakehouse-guid>
python scripts/configure_semantic_model.py
```

#### Dry Run Mode

Preview changes without modifying files:

```bash
python scripts/configure_semantic_model.py \
    --workspace-id <workspace-guid> \
    --lakehouse-id <lakehouse-guid> \
    --dry-run
```

### Finding Your Workspace and Lakehouse IDs

Both IDs are GUIDs that can be found in the Fabric portal:

1. Navigate to your workspace in Fabric
2. Open your lakehouse
3. Check the browser URL bar:
   ```
   https://app.fabric.microsoft.com/groups/{workspace-id}/lakehouses/{lakehouse-id}
   ```

### Example

```bash
# Configure with specific IDs
python scripts/configure_semantic_model.py \
    --workspace-id 5219ac70-71d4-4dfc-af32-5b8a6c29a471 \
    --lakehouse-id fc9ed7b6-6723-4116-8bf1-278135865270

# Output:
# Lakehouse connection update:
#   File: fabric/semantic_model/retail_model.SemanticModel/definition/expressions.tmdl
#   Old URL: https://onelake.dfs.fabric.microsoft.com/[old-ids]
#   New URL: https://onelake.dfs.fabric.microsoft.com/[new-ids]
#
# Success! Lakehouse connection updated.
#
# Next steps:
#   1. Open retail_model.pbip in Power BI Desktop
#   2. Refresh the semantic model to load data from your lakehouse
```

### Integration with Deployment Workflow

This script is typically used as part of the semantic model deployment process:

1. Run this configuration script to set the lakehouse connection
2. Import the semantic model into your Fabric workspace
3. Refresh the model to load data from your lakehouse

See `fabric/semantic_model/README.md` for complete deployment instructions.

### Error Handling

The script validates inputs and provides clear error messages:

- Missing workspace or lakehouse ID
- Invalid GUID format
- Missing expressions.tmdl file
- File read/write errors

### Testing

Run the test suite:

```bash
pytest tests/scripts/test_configure_semantic_model.py -v
```

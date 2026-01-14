# Semantic Model Deployment

Deploy the Power BI semantic model to enable analytics and dashboards.

## Prerequisites

- ✅ Gold layer tables created (Phase 4 complete)
- ✅ Fabric workspace with Lakehouse containing `au` and `ag` schemas
- ✅ Contributor or Admin access to workspace

## Deployment Methods

### Method 1: Fabric Portal (Recommended)

**Step 1: Get Your Lakehouse Resource ID**

1. Navigate to your Fabric workspace
2. Open your Lakehouse (the one containing `au` and `ag` schemas)
3. In the URL bar, copy the Lakehouse ID:
   ```
   https://app.fabric.microsoft.com/groups/{workspace-id}/lakehouses/{lakehouse-id}
   ```
4. Your resource ID format is:
   ```
   /subscriptions/{subscription}/resourceGroups/{rg}/providers/Microsoft.Fabric/workspaces/{workspace}/lakehouses/{lakehouse-id}
   ```

   **OR** simply use the short form:
   ```
   {lakehouse-id}
   ```

**Step 2: Update model.tmdl with Your Lakehouse ID**

```bash
cd fabric/semantic_model

# Edit model.tmdl and replace the placeholder
# Change this line:
#   value: "<fabric-lakehouse-resource-id>"
# To:
#   value: "your-lakehouse-id-here"
```

**Step 3: Create Semantic Model in Fabric**

1. Go to your Fabric workspace
2. Click **+ New** → **Semantic model**
3. Choose **Upload a .tmdl file**
4. Select `fabric/semantic_model/model.tmdl`
5. Name it: `RetailGold`

**Step 4: Configure Data Sources (if needed)**

If the Lakehouse connection doesn't auto-configure:

1. Open the semantic model settings
2. Go to **Data source credentials**
3. Set credentials for the Lakehouse connection
4. Click **Apply**

**Step 5: Refresh the Model**

1. In the semantic model view, click **Refresh now**
2. Wait for refresh to complete (1-5 minutes)
3. Check for any errors in the refresh history

**Step 6: Verify Tables Loaded**

1. Open the semantic model in **Power BI Desktop** or **Fabric**
2. Check that all 11 tables appear:
   - 9 Gold tables (au schema)
   - 2 Dimension tables (ag schema)
3. Verify relationships are active
4. Test a simple measure: `Total Sales` from `gold_sales_minute_store`

### Method 2: Power BI Desktop + Publish

**Step 1: Open in Power BI Desktop**

1. Install [Power BI Desktop](https://aka.ms/pbidesktop) if not already installed
2. Open Power BI Desktop
3. Go to **File** → **Import** → **Power BI Model**
4. Browse to `fabric/semantic_model/model.tmdl`

**Step 2: Configure Lakehouse Connection**

1. Power BI will prompt for data source settings
2. Select **DirectLake** mode
3. Point to your Fabric Lakehouse:
   - Server: `https://api.fabric.microsoft.com/v1`
   - Database: Your Lakehouse name
   - Schemas: `au`, `ag`

**Step 3: Publish to Fabric**

1. Click **Publish** in Power BI Desktop ribbon
2. Select your Fabric workspace
3. Wait for publish to complete
4. Click **Open in Fabric** when done

### Method 3: Fabric Git Integration (Advanced)

If your workspace is connected to Git:

**Step 1: Commit Model to Git**

```bash
# Already done - model.tmdl is in the repo
git add fabric/semantic_model/model.tmdl
git commit -m "Update semantic model with current Gold tables"
git push
```

**Step 2: Sync in Fabric**

1. In your Fabric workspace, go to **Git integration**
2. Click **Update from Git**
3. Select the semantic model changes
4. Click **Update**

**Step 3: Configure and Refresh**

Follow steps 4-6 from Method 1

## Verification Checklist

After deployment, verify:

- [ ] All 11 tables visible in model
- [ ] 4 relationships active (check relationship view)
- [ ] Measures calculate correctly:
  - `Total Sales` shows values
  - `Avg Basket` shows reasonable values ($20-$200)
  - `On Hand` shows inventory counts
- [ ] Perspectives show correct tables:
  - Operations (6 tables)
  - Merchandising (4 tables)
  - Logistics (2 tables)
  - Marketing (1 table)
- [ ] Data refresh succeeds without errors

## Common Issues

### Issue: "Table not found" error

**Cause**: Gold tables don't exist in Lakehouse

**Solution**:
```bash
# Verify Gold tables exist
# In Fabric Lakehouse, run:
SHOW TABLES IN au;

# Should show 9 tables. If not, re-run:
# fabric/lakehouse/04-streaming-to-gold.ipynb
```

### Issue: "Cannot connect to data source"

**Cause**: Lakehouse credentials not configured

**Solution**:
1. Semantic model settings → Data source credentials
2. Edit credentials for Lakehouse
3. Use OAuth or Service Principal
4. Click Apply and refresh

### Issue: Refresh fails with "Memory limit exceeded"

**Cause**: Trying to Import mode instead of DirectLake

**Solution**:
1. Check partition mode in model.tmdl: `mode: DirectLake`
2. Ensure Fabric capacity supports DirectLake
3. If not, change to DirectQuery mode

### Issue: Tables show but no data

**Cause**: Gold tables are empty

**Solution**:
```bash
# Check if Gold tables have data
SELECT COUNT(*) FROM au.sales_minute_store;

# If 0, re-run historical load:
# fabric/lakehouse/02-historical-data-load.ipynb
```

## Performance Tuning

### DirectLake Mode Benefits
- **Sub-second queries**: No import lag
- **Always fresh**: Reflects latest Gold data
- **Low memory**: No data duplication

### Optimization Tips
1. **Aggregations**: Already pre-aggregated in Gold layer
2. **Relationships**: Keep to essential joins only
3. **Measures**: Use CALCULATE for time intelligence
4. **Partitioning**: Not needed for DirectLake

## Next Steps

After semantic model is deployed:

1. **Create Reports**: Use the semantic model as a data source
2. **Build Dashboards**: Add tiles using the model measures
3. **Schedule Refresh**: Set refresh schedule for Import mode tables (if any)
4. **Set Up Alerts**: Configure data-driven alerts

Proceed to [Phase 7: Dashboards](07-dashboards.md) to build visualizations.

## References

- [Microsoft Fabric DirectLake Documentation](https://learn.microsoft.com/fabric/get-started/direct-lake-overview)
- [TMDL Format Reference](https://learn.microsoft.com/power-bi/developer/projects/projects-dataset)
- [Power BI Semantic Model Best Practices](https://learn.microsoft.com/power-bi/guidance/star-schema)

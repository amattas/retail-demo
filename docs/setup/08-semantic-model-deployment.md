# Semantic Model Deployment

Deploy the Power BI semantic model to enable analytics and dashboards.

## Prerequisites

- ✅ Gold layer tables created (Phase 4 complete)
- ✅ Fabric workspace with Lakehouse containing `au` and `ag` schemas
- ✅ Contributor or Admin access to workspace
- ✅ [Power BI Desktop](https://aka.ms/pbidesktop) installed

## Deployment Process

With the TMDL-based Power BI Project format (.pbip), deployment is straightforward using Power BI Desktop.

### Step 1: Open the Power BI Project

1. Launch **Power BI Desktop**
2. Go to **File** → **Open**
3. Browse to `fabric/semantic_model/retail_model.pbip`
4. Click **Open**

Power BI Desktop will load the semantic model and report together.

### Step 2: Configure Lakehouse Connection (First Time Only)

If this is your first time opening the project, you may need to configure the data source:

1. Power BI will prompt: "Unable to connect to data source"
2. Click **Edit** or go to **Transform data** → **Data source settings**
3. Select the Lakehouse connection
4. Click **Edit Permissions**
5. Set credentials:
   - **Authentication method**: OAuth 2.0
   - Sign in with your Fabric workspace account
6. Click **OK**

The model uses **DirectLake mode**, which connects directly to your Lakehouse Gold tables without importing data.

### Step 3: Refresh the Data (Optional)

To ensure you're seeing the latest data:

1. Click **Refresh** in the Home ribbon
2. Wait for refresh to complete (typically 10-30 seconds for DirectLake)

### Step 4: Publish to Fabric Workspace

1. Click **Publish** in the Home ribbon
2. Select your target Fabric workspace from the list
3. Click **Select**
4. Wait for the upload to complete
5. When prompted, click **Open in Fabric** to view in the browser

### Step 5: Verify Deployment

After publishing:

1. Navigate to your Fabric workspace
2. Verify two items were created:
   - **Semantic model**: `retail_model`
   - **Report**: `retail_model`
3. Open the semantic model to verify:
   - All 12 tables are present (9 Gold + 3 Dimension tables)
   - Relationships are active
   - Data refreshes successfully

### Alternative: Fabric Git Integration (Advanced)

If your workspace is connected to Git:

**Step 1: Commit Model to Git**

```bash
git add fabric/semantic_model/
git commit -m "Update Power BI semantic model"
git push
```

**Step 2: Sync in Fabric**

1. In your Fabric workspace, go to **Source control**
2. Click **Update all**
3. The semantic model and report will sync from Git

## Verification Checklist

After deployment, verify:

- [ ] All 12 tables visible in model (9 Gold + 3 Dimension tables)
- [ ] Relationships are active (check relationship view)
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
- [ ] Report pages display correctly in workspace

## Common Issues

### Issue: "Unable to connect to data source"

**Cause**: First time opening the .pbip file or Lakehouse credentials not configured

**Solution**:
1. In Power BI Desktop, click **Transform data** → **Data source settings**
2. Select the Lakehouse connection
3. Click **Edit Permissions** → **Edit**
4. Choose **OAuth 2.0** and sign in with your Fabric account
5. Click **OK** and close the dialogs
6. Click **Refresh** to reload data

### Issue: "Table not found" error

**Cause**: Gold tables don't exist in Lakehouse

**Solution**:
```bash
# Verify Gold tables exist in Fabric Lakehouse
# Run this query in the Lakehouse SQL endpoint:
SELECT table_schema, table_name
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('au', 'ag');

# Should show 12 tables. If not, re-run:
# fabric/lakehouse/04-streaming-to-gold.ipynb
```

### Issue: Refresh fails with "Memory limit exceeded"

**Cause**: Trying to Import mode instead of DirectLake

**Solution**:
1. The model uses DirectLake mode by default (no import)
2. Ensure your Fabric capacity supports DirectLake (F64 or higher recommended)
3. Check capacity settings in Fabric admin portal

### Issue: Tables show but no data

**Cause**: Gold tables are empty or not loaded

**Solution**:
```bash
# Check if Gold tables have data via Lakehouse SQL endpoint:
SELECT COUNT(*) FROM au.gold_sales_minute_store;

# If 0, run the historical load notebook:
# fabric/lakehouse/02-historical-data-load.ipynb
```

### Issue: "Can't publish - semantic model already exists"

**Cause**: A semantic model with the same name already exists in the workspace

**Solution**:
1. **Option A**: In Power BI Desktop, choose **Replace** when prompted
2. **Option B**: Rename the model before publishing (File → Save As)
3. **Option C**: Delete the old semantic model from the workspace first

## Model Features

### DirectLake Mode Benefits
- **Sub-second queries**: No import lag, direct query to Lakehouse
- **Always fresh**: Reflects latest Gold data in near-real-time
- **Low memory**: No data duplication or caching needed
- **Auto schema sync**: Table changes in Lakehouse reflect automatically

### Pre-built Report Pages

The `retail_model.pbip` includes five report pages ready to use:

1. **Sales Dashboard**: Sales KPIs, store breakdowns, category performance
2. **Supply Chain Control Tower**: Store/DC inventory, reorders, truck dwell trends
3. **Online, Payments & Marketing**: Online sales, tender mix, marketing spend
4. **Inventory & Replenishment**: On-hand units/value, reorder priorities
5. **Logistics Control Center**: Truck dwell metrics, DC throughput

You can customize these reports or build new ones using the semantic model.

### Optimization Tips
1. **Aggregations**: Pre-aggregated in Gold layer (no additional aggregation needed)
2. **Relationships**: Defined between Gold tables and dimensions
3. **Measures**: Built-in DAX measures for common KPIs
4. **Perspectives**: Organized by business area (Operations, Merchandising, Logistics, Marketing)

## Next Steps

After the semantic model is deployed:

1. **Explore Reports**: Review the five pre-built report pages in the workspace
2. **Customize Visuals**: Modify charts, filters, and layouts as needed
3. **Share with Users**: Assign workspace roles or share the report
4. **Pin to Dashboard**: Create a dashboard by pinning key visuals

Proceed to [Phase 7: Dashboards](07-dashboards.md) for additional dashboard guidance.

## References

- [Microsoft Fabric DirectLake Documentation](https://learn.microsoft.com/fabric/get-started/direct-lake-overview)
- [TMDL Format Reference](https://learn.microsoft.com/power-bi/developer/projects/projects-dataset)
- [Power BI Semantic Model Best Practices](https://learn.microsoft.com/power-bi/guidance/star-schema)

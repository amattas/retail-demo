# Microsoft Fabric Security & Access Control Guide

This guide covers security configuration, workspace roles, and permissions required for the Retail Demo medallion architecture.

---

## Table of Contents

1. [Workspace Roles & Permissions](#workspace-roles--permissions)
2. [Data Source Permissions](#data-source-permissions)
3. [Service Principal Setup](#service-principal-setup)
4. [Row-Level Security (RLS)](#row-level-security-rls)
5. [Security Best Practices](#security-best-practices)
6. [Troubleshooting Access Issues](#troubleshooting-access-issues)

---

## Workspace Roles & Permissions

### Fabric Workspace Roles

Microsoft Fabric workspaces support four roles with different permission levels:

| Role | Permissions | Use Case |
|------|-------------|----------|
| **Admin** | Full control: manage workspace, assign roles, delete items | Workspace owners, DevOps automation |
| **Member** | Create/edit/delete items, publish reports | Developers, data engineers |
| **Contributor** | Create/edit items, run notebooks/pipelines | Data analysts, notebook developers |
| **Viewer** | Read-only access to reports and datasets | Business users, report consumers |

### Required Roles by Task

| Task | Minimum Role | Notes |
|------|--------------|-------|
| Deploy Bronze shortcuts notebook | **Contributor** | Needs to create/edit notebooks |
| Run Bronze → Silver pipeline | **Contributor** | Execute notebooks via pipeline |
| Create semantic models | **Member** | Publish Power BI datasets |
| View dashboards | **Viewer** | Read-only access sufficient |
| Manage workspace settings | **Admin** | Assign roles, configure capacity |
| Deploy via CI/CD | **Member** or service principal | Automated deployment |

### Assigning Workspace Roles

**Via Fabric Portal:**
1. Navigate to workspace settings (gear icon)
2. Select **Access**
3. Click **Add people or groups**
4. Enter user email or Azure AD group
5. Select role (Admin/Member/Contributor/Viewer)
6. Click **Add**

**Via PowerShell:**
```powershell
# Add user to workspace
Add-PowerBIWorkspaceUser `
    -Scope Organization `
    -Id <workspace-id> `
    -UserPrincipalName user@domain.com `
    -AccessRight Contributor
```

---

## Data Source Permissions

### Azure Data Lake Storage Gen2 (ADLS)

Bronze layer shortcuts require read access to ADLS parquet files.

**Required Permissions:**
- **Storage Blob Data Reader** role on storage account or container
- Network access if storage firewall enabled

**Assign RBAC via Azure Portal:**
1. Navigate to storage account → **Access Control (IAM)**
2. Click **Add role assignment**
3. Select **Storage Blob Data Reader**
4. Assign to:
   - Fabric workspace service principal (for automated access)
   - Individual users (for interactive development)
5. Click **Save**

**Assign RBAC via Azure CLI:**
```bash
# For individual user
az role assignment create \
    --role "Storage Blob Data Reader" \
    --assignee user@domain.com \
    --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<account>

# For service principal
az role assignment create \
    --role "Storage Blob Data Reader" \
    --assignee <service-principal-object-id> \
    --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<account>
```

**Firewall Configuration:**
- If ADLS has network restrictions, add Fabric workspace IP ranges
- Or enable "Allow Azure services on the trusted services list"

### Azure Event Hubs

Eventhouse shortcuts and data ingestion require Event Hubs permissions.

**Required Permissions:**
- **Azure Event Hubs Data Receiver** (for Eventhouse ingestion)
- **Azure Event Hubs Data Sender** (for datagen streaming)

**Assign via Azure Portal:**
1. Navigate to Event Hubs namespace → **Access Control (IAM)**
2. Add role assignment for **Azure Event Hubs Data Receiver**
3. Assign to Eventhouse cluster service principal
4. Add role assignment for **Azure Event Hubs Data Sender**
5. Assign to datagen application or user

### Eventhouse (KQL Database)

Bronze layer streaming shortcuts require read access to Eventhouse tables.

**Required Permissions:**
- **Database User** role on KQL database (minimum)
- **Database Viewer** role (read-only, recommended for shortcuts)

**Assign via Fabric Portal:**
1. Open Eventhouse item
2. Navigate to **Manage** → **Permissions**
3. Click **Add**
4. Select user or service principal
5. Choose **Viewer** role
6. Click **Add**

**Assign via KQL:**
```kql
// Grant viewer access
.add database retail_eventhouse viewers ('aaduser=user@domain.com')

// Grant user access (includes write)
.add database retail_eventhouse users ('aaduser=user@domain.com')

// Grant to service principal
.add database retail_eventhouse viewers ('aadapp=<app-id>')
```

---

## Service Principal Setup

For automated CI/CD deployments and pipeline execution, use Azure AD service principals instead of user accounts.

### Create Service Principal

**Via Azure Portal:**
1. Navigate to **Azure Active Directory** → **App registrations**
2. Click **New registration**
3. Enter name: `retail-demo-fabric-automation`
4. Select supported account types
5. Click **Register**
6. Note **Application (client) ID** and **Directory (tenant) ID**

**Via Azure CLI:**
```bash
az ad sp create-for-rbac \
    --name retail-demo-fabric-automation \
    --role Contributor \
    --scopes /subscriptions/<sub-id>/resourceGroups/<rg>
```

### Create Client Secret

1. Navigate to app registration → **Certificates & secrets**
2. Click **New client secret**
3. Enter description: `Fabric deployment key`
4. Select expiration (1 year recommended)
5. Click **Add**
6. **Copy secret value immediately** (won't be shown again)

### Assign Permissions

Assign the service principal to:
- **Fabric workspace**: Member role (for deployment)
- **ADLS storage account**: Storage Blob Data Reader
- **Event Hubs namespace**: Azure Event Hubs Data Receiver

### Use in CI/CD Pipelines

**GitHub Actions example:**
```yaml
- name: Login to Azure
  uses: azure/login@v1
  with:
    creds: ${{ secrets.AZURE_CREDENTIALS }}

- name: Deploy Fabric artifacts
  run: |
    # Use Azure CLI or Fabric REST API
    az fabric pipeline create ...
```

**Azure DevOps example:**
```yaml
- task: AzureCLI@2
  inputs:
    azureSubscription: 'retail-demo-service-connection'
    scriptType: 'bash'
    scriptLocation: 'inlineScript'
    inlineScript: |
      # Deploy using service principal auth
      az fabric pipeline create ...
```

---

## Row-Level Security (RLS)

Implement RLS in the semantic model to restrict data access by store, region, or customer segment.

### Define RLS Roles in Semantic Model

**Example: Store-level access**
```dax
// In model.tmdl or via Power BI Desktop
role: StoreManagers
  tablePermission:
    table: fact_receipts
    filterExpression: 
      [store_id] IN (
        LOOKUPVALUE(
          dim_stores[ID],
          dim_stores[manager_email],
          USERPRINCIPALNAME()
        )
      )
```

**Example: Region-level access**
```dax
role: RegionalManagers
  tablePermission:
    table: fact_receipts
    filterExpression:
      RELATED(dim_stores[region]) = "West"
```

### Assign Users to RLS Roles

**Via Power BI Service:**
1. Navigate to semantic model settings
2. Select **Security** → **Row-level security**
3. Select role (e.g., `StoreManagers`)
4. Click **Add members**
5. Enter user emails or Azure AD groups
6. Click **Add**

### Test RLS

**Via Power BI Desktop:**
1. Open report
2. Click **Modeling** → **View As**
3. Select roles to test
4. Verify data is filtered correctly

**Via Power BI Service:**
- Use "View as role" feature in semantic model settings

---

## Security Best Practices

### 1. Principle of Least Privilege
- Grant minimum permissions required for each role
- Use Viewer role for report consumers
- Restrict Admin role to workspace owners only

### 2. Use Azure AD Groups
- Create AD groups for each role (e.g., `retail-demo-developers`, `retail-demo-viewers`)
- Assign groups to workspace roles instead of individual users
- Simplifies permission management and auditing

### 3. Service Principal Hygiene
- Rotate client secrets annually
- Use separate service principals for dev/staging/prod
- Store secrets in Azure Key Vault, not in code
- Audit service principal usage regularly

### 4. Network Security
- Enable ADLS firewall and allow only Fabric IP ranges
- Use private endpoints for Event Hubs if required
- Restrict Fabric workspace to specific Azure AD tenants

### 5. Data Encryption
- ADLS uses encryption at rest by default (Microsoft-managed keys)
- Enable customer-managed keys (CMK) for additional control
- Fabric data in lakehouse is encrypted automatically

### 6. Auditing & Monitoring
- Enable Azure AD audit logs for workspace access changes
- Monitor ADLS access logs for unusual activity
- Use Fabric monitoring to track pipeline execution and data access
- Set up alerts for permission changes or failed authentications

### 7. Sensitive Data Protection
- Avoid storing PII in plain text (use hashing or encryption)
- Implement RLS for customer or employee data
- Use dynamic data masking in Eventhouse for sensitive fields
- Comply with GDPR, CCPA, or other privacy regulations

### 8. Credential Management
- **Never** hardcode connection strings in notebooks
- Use environment variables for configuration
- Store secrets in Azure Key Vault
- Reference Key Vault from Fabric workspace settings

---

## Troubleshooting Access Issues

### "Permission denied" when accessing ADLS

**Symptoms:**
- Bronze shortcut creation fails with "Access denied"
- Notebook errors: "AnalysisException: Permission denied"

**Causes:**
- Missing **Storage Blob Data Reader** role
- ADLS firewall blocking Fabric IP ranges
- Workspace managed identity not enabled

**Solutions:**
1. Verify RBAC assignment on storage account
2. Check ADLS firewall settings → Allow Azure services
3. Enable managed identity for Fabric workspace
4. Test access using Azure Storage Explorer with same credentials

### "Table does not exist" when accessing Eventhouse

**Symptoms:**
- Bronze shortcut shows empty or fails to load
- Error: "Table 'cusn.receipt_created' not found"

**Causes:**
- Missing **Database Viewer** role on Eventhouse
- Eventhouse shortcut not created (manual step required)
- Network connectivity issues

**Solutions:**
1. Verify Eventhouse permissions via KQL: `.show database retail_eventhouse principals`
2. Manually create Eventhouse shortcuts via Fabric Portal
3. Test connectivity: Run query in Eventhouse KQL Queryset
4. Check Eventhouse cluster is running (not paused)

### "Forbidden" when running pipeline

**Symptoms:**
- Pipeline fails with 403 Forbidden error
- Activity execution denied

**Causes:**
- Insufficient workspace role (Viewer cannot execute)
- Pipeline tries to access external resource without permission
- Managed identity not configured

**Solutions:**
1. Verify user has **Contributor** or **Member** role
2. Check pipeline activity permissions (e.g., notebook execution)
3. Enable managed identity if pipeline accesses external services
4. Review pipeline activity logs for specific error details

### Users cannot see reports/dashboards

**Symptoms:**
- Report not visible in workspace
- Error: "You don't have permission to view this report"

**Causes:**
- User not assigned to workspace
- Report not published or shared
- RLS filtering all data for user

**Solutions:**
1. Assign user to workspace with **Viewer** role minimum
2. Verify report is published (not in draft mode)
3. Check RLS rules aren't overly restrictive
4. Test with admin account to isolate RLS vs permissions

### Service principal authentication fails

**Symptoms:**
- CI/CD pipeline fails with "Authentication failed"
- Error: "AADSTS7000215: Invalid client secret"

**Causes:**
- Client secret expired
- Incorrect tenant ID or app ID
- Service principal not assigned to workspace

**Solutions:**
1. Verify client secret expiration date in Azure AD
2. Generate new client secret if expired
3. Confirm tenant ID, app ID, and secret are correct
4. Assign service principal to Fabric workspace
5. Wait 10-15 minutes for permission propagation

---

## Related Documentation

- [Microsoft Fabric Workspace Roles](https://learn.microsoft.com/fabric/get-started/roles-workspaces)
- [Azure RBAC for Storage](https://learn.microsoft.com/azure/storage/blobs/assign-azure-role-data-access)
- [Event Hubs Authorization](https://learn.microsoft.com/azure/event-hubs/authorize-access-azure-active-directory)
- [Power BI Row-Level Security](https://learn.microsoft.com/power-bi/admin/service-admin-rls)

---

## Quick Reference: Permission Matrix

| Resource | Role | Permissions | Assignment Method |
|----------|------|-------------|-------------------|
| **Fabric Workspace** | Viewer | Read reports | Workspace → Access |
| **Fabric Workspace** | Contributor | Run notebooks | Workspace → Access |
| **Fabric Workspace** | Member | Create/publish items | Workspace → Access |
| **Fabric Workspace** | Admin | Full control | Workspace → Access |
| **ADLS Container** | Storage Blob Data Reader | Read parquet files | Azure Portal → IAM |
| **Event Hubs** | Data Receiver | Ingest events | Azure Portal → IAM |
| **Eventhouse** | Viewer | Read tables | Eventhouse → Permissions |
| **Semantic Model** | RLS Role | Row-filtered data | Power BI → Security |

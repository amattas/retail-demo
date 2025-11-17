# Credential Management Guide

Comprehensive guide for securely managing Azure Event Hub credentials and sensitive configuration.

## Table of Contents

- [Security Principles](#security-principles)
- [Credential Storage Methods](#credential-storage-methods)
- [Environment Variables](#environment-variables)
- [Azure Key Vault](#azure-key-vault)
- [Configuration Files](#configuration-files)
- [CI/CD Integration](#cicd-integration)
- [Rotation & Revocation](#rotation--revocation)
- [Audit & Compliance](#audit--compliance)

---

## Security Principles

### Never Commit Credentials

**Critical Rule**: Never commit credentials to version control.

**Why?**
- Credentials in git history remain accessible even after deletion
- Public repositories expose credentials to the world
- Automated bots scan GitHub for credentials within minutes
- Compliance violations (SOC 2, ISO 27001, PCI DSS)

**Prevention:**

1. **Add to .gitignore:**
   ```bash
   echo 'config.json' >> .gitignore
   echo '.env' >> .gitignore
   echo '*.secret' >> .gitignore
   ```

2. **Use git-secrets:**
   ```bash
   # Install git-secrets
   brew install git-secrets

   # Configure for repository
   cd /path/to/retail-datagen
   git secrets --install
   git secrets --register-aws
   ```

3. **Pre-commit hooks:**
   ```bash
   # .git/hooks/pre-commit
   #!/bin/bash
   if git diff --cached | grep -i "SharedAccessKey"; then
     echo "ERROR: Attempting to commit Azure credentials!"
     exit 1
   fi
   ```

### Principle of Least Privilege

**Grant minimum necessary permissions:**

- **Development**: Send-only permissions
- **Production**: Send-only with specific Event Hub access
- **Monitoring**: Listen-only permissions
- **Management**: Separate credentials for admin operations

**Azure Event Hub Access Policies:**

```
Development Policy (Send Only):
- Permissions: Send
- Event Hub: retail-events-dev

Production Policy (Send Only):
- Permissions: Send
- Event Hub: retail-events-prod

Monitoring Policy (Listen Only):
- Permissions: Listen
- Event Hub: retail-events-prod
```

### Defense in Depth

**Multiple layers of security:**

1. **Network**: Firewall rules, private endpoints
2. **Identity**: Azure Active Directory, Managed Identities
3. **Access**: RBAC, access policies
4. **Audit**: Logging, monitoring, alerts
5. **Encryption**: TLS 1.2+, encryption at rest

---

## Credential Storage Methods

### Method Comparison

| Method | Security | Complexity | Best For |
|--------|----------|------------|----------|
| Environment Variables | Medium | Low | Development, CI/CD |
| Azure Key Vault | High | Medium | Production, Enterprise |
| Configuration Files | Low | Low | Local development only |
| Managed Identity | Highest | Medium | Azure deployments |
| Azure Storage (Upload) | Medium | Low | Optional upload after export |

---

## Environment Variables

### Overview

Environment variables are the recommended method for development and CI/CD pipelines.

**Advantages:**
- Simple to use
- No files to manage
- Easy to rotate
- Supported by all deployment platforms

**Disadvantages:**
- Visible in process listings
- Not encrypted at rest
- Can be leaked via error messages

### Setting Environment Variables

#### Linux/macOS (Bash/Zsh)

**Temporary (current session only):**
```bash
export AZURE_EVENTHUB_CONNECTION_STRING="Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key;EntityPath=retail-events"
```

**Persistent (add to shell profile):**
```bash
# Add to ~/.bashrc, ~/.zshrc, or ~/.profile
echo 'export AZURE_EVENTHUB_CONNECTION_STRING="Endpoint=sb://..."' >> ~/.bashrc
source ~/.bashrc
```

**Verify:**
```bash
echo $AZURE_EVENTHUB_CONNECTION_STRING
```

### Azure Storage for Uploads (Optional)

Used by the "Upload Dimensions/Facts" actions after export.

```bash
# Account URI (optionally include container/prefix)
export AZURE_STORAGE_ACCOUNT_URI="https://<account>.blob.core.windows.net[/<container>[/<prefix>]]"

# Account key
export AZURE_STORAGE_ACCOUNT_KEY="<storage-account-key>"
```

You may also set these in `config.json` under `storage`.

#### Windows (PowerShell)

**Temporary (current session only):**
```powershell
$env:AZURE_EVENTHUB_CONNECTION_STRING = "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key;EntityPath=retail-events"
```

**Persistent (user scope):**
```powershell
[System.Environment]::SetEnvironmentVariable('AZURE_EVENTHUB_CONNECTION_STRING', 'Endpoint=sb://...', 'User')
```

**Persistent (system scope - requires admin):**
```powershell
[System.Environment]::SetEnvironmentVariable('AZURE_EVENTHUB_CONNECTION_STRING', 'Endpoint=sb://...', 'Machine')
```

**Verify:**
```powershell
$env:AZURE_EVENTHUB_CONNECTION_STRING
```

#### Docker

**Pass at runtime:**
```bash
docker run -d \
  --name retail-datagen \
  -p 8000:8000 \
  -e AZURE_EVENTHUB_CONNECTION_STRING="$AZURE_EVENTHUB_CONNECTION_STRING" \
  retail-datagen
```

**Using .env file:**
```bash
# Create .env file (don't commit!)
cat > .env <<EOF
AZURE_EVENTHUB_CONNECTION_STRING=Endpoint=sb://...
EOF

# Run with .env file
docker run -d \
  --name retail-datagen \
  -p 8000:8000 \
  --env-file .env \
  retail-datagen
```

#### Kubernetes

**Using Secrets:**
```bash
# Create secret from literal
kubectl create secret generic azure-credentials \
  --from-literal=connection-string="Endpoint=sb://..."

# Create secret from file
echo "Endpoint=sb://..." > connection-string.txt
kubectl create secret generic azure-credentials \
  --from-file=connection-string=connection-string.txt
rm connection-string.txt
```

**Deployment manifest:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: retail-datagen
spec:
  template:
    spec:
      containers:
      - name: retail-datagen
        image: retail-datagen:latest
        env:
        - name: AZURE_EVENTHUB_CONNECTION_STRING
          valueFrom:
            secretKeyRef:
              name: azure-credentials
              key: connection-string
```

### Best Practices for Environment Variables

1. **Use strong naming conventions:**
   - Prefix with application name: `RETAIL_DATAGEN_*`
   - Uppercase with underscores: `AZURE_EVENTHUB_CONNECTION_STRING`

2. **Validate presence at startup:**
   ```python
   import os

   conn_str = os.getenv('AZURE_EVENTHUB_CONNECTION_STRING')
   if not conn_str:
       raise ValueError("AZURE_EVENTHUB_CONNECTION_STRING not set")
   ```

3. **Don't log environment variables:**
   ```python
   # Bad - logs credential
   logger.info(f"Connection string: {conn_str}")

   # Good - masks credential
   logger.info("Connection string: [REDACTED]")
   ```

4. **Clear after use (if sensitive):**
   ```bash
   unset AZURE_EVENTHUB_CONNECTION_STRING
   ```

---

## Azure Key Vault

### Overview

Azure Key Vault is the recommended solution for production environments.

**Advantages:**
- Centralized credential management
- Encryption at rest and in transit
- Audit logging
- Access policies and RBAC
- Automatic rotation support

**Disadvantages:**
- Requires Azure subscription
- Additional setup complexity
- Network dependency

### Setup

#### 1. Create Key Vault

```bash
# Variables
RESOURCE_GROUP="retail-datagen-rg"
LOCATION="eastus"
VAULT_NAME="retail-datagen-kv"

# Create resource group
az group create --name $RESOURCE_GROUP --location $LOCATION

# Create Key Vault
az keyvault create \
  --name $VAULT_NAME \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --enable-rbac-authorization false
```

#### 2. Store Connection String

```bash
# Store secret
az keyvault secret set \
  --vault-name $VAULT_NAME \
  --name "eventhub-connection-string" \
  --value "Endpoint=sb://your-namespace.servicebus.windows.net/;..."
```

#### 3. Grant Access

**Option A: Managed Identity (Recommended)**

```bash
# Enable system-assigned managed identity for VM/App Service
az vm identity assign --name myVM --resource-group $RESOURCE_GROUP

# Grant Key Vault access
IDENTITY_ID=$(az vm show --name myVM --resource-group $RESOURCE_GROUP --query identity.principalId -o tsv)

az keyvault set-policy \
  --name $VAULT_NAME \
  --object-id $IDENTITY_ID \
  --secret-permissions get list
```

**Option B: Service Principal**

```bash
# Create service principal
az ad sp create-for-rbac --name retail-datagen-sp

# Grant Key Vault access
SP_OBJECT_ID=$(az ad sp show --id <app-id> --query id -o tsv)

az keyvault set-policy \
  --name $VAULT_NAME \
  --object-id $SP_OBJECT_ID \
  --secret-permissions get list
```

### Application Integration

#### Install Dependencies

```bash
pip install azure-keyvault-secrets azure-identity
```

#### Python Code

```python
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Using Managed Identity (recommended)
credential = DefaultAzureCredential()
vault_url = "https://retail-datagen-kv.vault.azure.net/"
client = SecretClient(vault_url=vault_url, credential=credential)

# Retrieve secret
connection_string = client.get_secret("eventhub-connection-string").value
```

#### Configuration

Update `config.json`:

```json
{
  "realtime": {
    "use_keyvault": true,
    "keyvault_url": "https://retail-datagen-kv.vault.azure.net/",
    "keyvault_secret_name": "eventhub-connection-string"
  }
}
```

### Best Practices for Key Vault

1. **Enable soft delete:**
   ```bash
   az keyvault update --name $VAULT_NAME --enable-soft-delete true
   ```

2. **Enable purge protection:**
   ```bash
   az keyvault update --name $VAULT_NAME --enable-purge-protection true
   ```

3. **Use RBAC instead of access policies:**
   ```bash
   az keyvault create --name $VAULT_NAME --enable-rbac-authorization true
   ```

4. **Monitor access:**
   ```bash
   az monitor diagnostic-settings create \
     --name KeyVaultAudit \
     --resource /subscriptions/{sub-id}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/{vault} \
     --logs '[{"category": "AuditEvent", "enabled": true}]' \
     --workspace /subscriptions/{sub-id}/resourcegroups/{rg}/providers/microsoft.operationalinsights/workspaces/{workspace}
   ```

5. **Implement caching:**
   ```python
   # Cache secrets to reduce Key Vault calls
   from functools import lru_cache

   @lru_cache(maxsize=1)
   def get_connection_string():
       return client.get_secret("eventhub-connection-string").value
   ```

---

## Configuration Files

### Local Development Only

Configuration files should only be used for local development and must never be committed.

### Template Pattern

**Commit a template:**

```json
// config.template.json
{
  "realtime": {
    "azure_connection_string": "<YOUR_CONNECTION_STRING_HERE>",
    "emit_interval_ms": 500,
    "burst": 100
  }
}
```

**Create local config from template:**

```bash
cp config.template.json config.json
# Edit config.json with real credentials
# config.json is in .gitignore
```

### Encryption at Rest

For additional security, encrypt configuration files:

```bash
# Encrypt config file
openssl enc -aes-256-cbc -salt -in config.json -out config.json.enc

# Decrypt when needed
openssl enc -aes-256-cbc -d -in config.json.enc -out config.json
```

**Store encryption key separately** (environment variable or Key Vault).

### File Permissions

Restrict access to configuration files:

```bash
# Owner read/write only
chmod 600 config.json

# Verify permissions
ls -la config.json
# -rw------- 1 user user 1234 Jan 15 10:00 config.json
```

---

## CI/CD Integration

### GitHub Actions

**Using GitHub Secrets:**

```yaml
name: Deploy
on: [push]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -e .

      - name: Run tests with credentials
        env:
          AZURE_EVENTHUB_CONNECTION_STRING: ${{ secrets.AZURE_EVENTHUB_CONNECTION_STRING }}
        run: pytest
```

**Add secret to GitHub:**
1. Repository → Settings → Secrets and variables → Actions
2. Click "New repository secret"
3. Name: `AZURE_EVENTHUB_CONNECTION_STRING`
4. Value: Your connection string
5. Click "Add secret"

### Azure DevOps

**Using Variable Groups:**

```yaml
trigger:
  - main

pool:
  vmImage: 'ubuntu-latest'

variables:
  - group: retail-datagen-secrets

steps:
- task: UsePythonVersion@0
  inputs:
    versionSpec: '3.11'

- script: |
    pip install -e .
  displayName: 'Install dependencies'

- script: |
    pytest
  env:
    AZURE_EVENTHUB_CONNECTION_STRING: $(AZURE_EVENTHUB_CONNECTION_STRING)
  displayName: 'Run tests'
```

**Create variable group:**
1. Pipelines → Library → Variable groups
2. Click "Add variable group"
3. Name: `retail-datagen-secrets`
4. Add variable: `AZURE_EVENTHUB_CONNECTION_STRING`
5. Lock icon: Make secret
6. Save

### GitLab CI/CD

**Using CI/CD Variables:**

```yaml
test:
  stage: test
  image: python:3.11
  before_script:
    - pip install -e .
  script:
    - pytest
  variables:
    AZURE_EVENTHUB_CONNECTION_STRING: $AZURE_EVENTHUB_CONNECTION_STRING
```

**Add CI/CD variable:**
1. Settings → CI/CD → Variables
2. Click "Add variable"
3. Key: `AZURE_EVENTHUB_CONNECTION_STRING`
4. Value: Your connection string
5. Flags: Protected, Masked
6. Save

---

## Rotation & Revocation

### Regular Rotation

**Recommended schedule:**
- Development: Every 6 months
- Production: Every 3 months
- After personnel changes: Immediately

### Rotation Process

#### 1. Generate New Key

```bash
# Regenerate primary key
az eventhubs namespace authorization-rule keys renew \
  --resource-group $RESOURCE_GROUP \
  --namespace-name $NAMESPACE \
  --name RootManageSharedAccessKey \
  --key PrimaryKey

# Get new connection string
az eventhubs namespace authorization-rule keys list \
  --resource-group $RESOURCE_GROUP \
  --namespace-name $NAMESPACE \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString -o tsv
```

#### 2. Update Credentials

**Environment variables:**
```bash
export AZURE_EVENTHUB_CONNECTION_STRING="<new-connection-string>"
```

**Key Vault:**
```bash
az keyvault secret set \
  --vault-name $VAULT_NAME \
  --name "eventhub-connection-string" \
  --value "<new-connection-string>"
```

#### 3. Restart Applications

```bash
# Docker
docker restart retail-datagen

# Kubernetes
kubectl rollout restart deployment/retail-datagen

# Systemd
sudo systemctl restart retail-datagen
```

#### 4. Verify

```bash
curl -X POST http://localhost:8000/api/stream/test
```

### Emergency Revocation

If credentials are compromised:

1. **Immediately regenerate both keys:**
   ```bash
   az eventhubs namespace authorization-rule keys renew \
     --key PrimaryKey
   az eventhubs namespace authorization-rule keys renew \
     --key SecondaryKey
   ```

2. **Review audit logs:**
   ```bash
   az monitor activity-log list \
     --resource-group $RESOURCE_GROUP \
     --start-time $(date -u -d '1 hour ago' '+%Y-%m-%dT%H:%M:%SZ')
   ```

3. **Notify security team**

4. **Update all applications**

5. **Document incident**

---

## Audit & Compliance

### Enable Audit Logging

**Azure Event Hub diagnostic settings:**

```bash
az monitor diagnostic-settings create \
  --name EventHubAudit \
  --resource /subscriptions/{sub-id}/resourceGroups/{rg}/providers/Microsoft.EventHub/namespaces/{namespace} \
  --logs '[
    {"category": "ArchiveLogs", "enabled": true},
    {"category": "OperationalLogs", "enabled": true}
  ]' \
  --workspace /subscriptions/{sub-id}/resourcegroups/{rg}/providers/microsoft.operationalinsights/workspaces/{workspace}
```

**Key Vault audit logging:**

```bash
az monitor diagnostic-settings create \
  --name KeyVaultAudit \
  --resource /subscriptions/{sub-id}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/{vault} \
  --logs '[{"category": "AuditEvent", "enabled": true}]' \
  --workspace /subscriptions/{sub-id}/resourcegroups/{rg}/providers/microsoft.operationalinsights/workspaces/{workspace}
```

### Query Audit Logs

**Azure Monitor Logs (KQL):**

```kql
// Key Vault access logs
AzureDiagnostics
| where ResourceType == "VAULTS"
| where OperationName == "SecretGet"
| where ResultSignature == "OK"
| project TimeGenerated, CallerIPAddress, Resource, OperationName
| order by TimeGenerated desc

// Event Hub operations
AzureDiagnostics
| where ResourceType == "EVENTHUBS"
| where Category == "OperationalLogs"
| project TimeGenerated, OperationName, ResultDescription
| order by TimeGenerated desc
```

### Compliance Reports

**Generate credential usage report:**

```bash
#!/bin/bash

echo "=== Credential Audit Report ==="
echo "Generated: $(date)"
echo ""

echo "=== Key Vault Secrets ==="
az keyvault secret list --vault-name $VAULT_NAME --query "[].{Name:name, Created:attributes.created, Updated:attributes.updated}" -o table

echo ""
echo "=== Event Hub Access Policies ==="
az eventhubs namespace authorization-rule list \
  --resource-group $RESOURCE_GROUP \
  --namespace-name $NAMESPACE \
  --query "[].{Name:name, Rights:rights}" -o table

echo ""
echo "=== Recent Key Vault Access ==="
az monitor activity-log list \
  --resource-group $RESOURCE_GROUP \
  --start-time $(date -u -d '7 days ago' '+%Y-%m-%dT%H:%M:%SZ') \
  --query "[?contains(resourceId, 'vaults')].{Time:eventTimestamp, Operation:operationName.value, Status:status.value}" -o table
```

### Compliance Checklist

- [ ] Credentials not in version control
- [ ] Environment variables or Key Vault used
- [ ] Least privilege access policies configured
- [ ] Audit logging enabled
- [ ] Rotation schedule defined
- [ ] Emergency revocation procedure documented
- [ ] Access reviewed quarterly
- [ ] Compliance requirements met (SOC 2, ISO 27001, etc.)

---

## Security Incident Response

### If Credentials Are Leaked

1. **Immediately revoke compromised credentials**
2. **Generate new credentials**
3. **Update all applications**
4. **Review logs for unauthorized access**
5. **Notify security team and stakeholders**
6. **Document incident and lessons learned**
7. **Implement additional controls**

### Prevention Checklist

- [ ] Pre-commit hooks prevent credential commits
- [ ] `.gitignore` configured correctly
- [ ] Secrets scanning enabled (GitHub, GitLab, etc.)
- [ ] Regular security training for team
- [ ] Incident response plan documented
- [ ] Regular credential rotation schedule

---

## Resources

- [Azure Key Vault Documentation](https://docs.microsoft.com/en-us/azure/key-vault/)
- [Azure Event Hubs Security](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-security-controls)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [NIST SP 800-57: Key Management](https://csrc.nist.gov/publications/detail/sp/800-57-part-1/rev-5/final)

---

## Next Steps

- **Setup**: See [STREAMING_SETUP.md](STREAMING_SETUP.md) for initial configuration
- **Operations**: See [STREAMING_OPERATIONS.md](STREAMING_OPERATIONS.md) for monitoring
- **API Reference**: See [STREAMING_API.md](STREAMING_API.md) for endpoint documentation

These JSON files define KQL ingestion mappings for Eventstream → KQL DB.

Usage in Kusto Explorer/Fabric KQL DB:
- Create table first (see `../tables.kql`).
- Then create a JSON mapping named `mapping-json` per table:

```
.create-or-alter table <table_name> ingestion json mapping 'mapping-json' '<paste-json-array-here>'
```

Event envelope fields map from `$.<field>`; payload fields map from `$.payload.<field>`.


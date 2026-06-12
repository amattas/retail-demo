# Fabric Deployment Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deployment framework under `deploy\` for provisioning Fabric resources with Terraform and deploying staged Fabric artifacts with fabric-cicd.

**Architecture:** Terraform owns environment resources and outputs deployed item IDs. Python helpers load one canonical YAML configuration, generate Terraform/fabric-cicd files, stage Fabric source assets, and validate output without requiring Fabric access.

**Tech Stack:** Python 3.11+, PyYAML, pytest, Terraform, microsoft/terraform-provider-fabric, microsoft/fabric-cicd.

---

### Task 1: Config and artifact helpers

**Files:**
- Create: `deploy\scripts\deploy_config.py`
- Create: `deploy\scripts\build_artifacts.py`
- Test: `tests\deploy\test_deploy_config.py`
- Test: `tests\deploy\test_build_artifacts.py`

- [x] **Step 1: Write failing tests for config loading and artifact staging**

Run: `python -m pytest tests\deploy -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'deploy.scripts'`.

- [x] **Step 2: Implement config loading and rendering**

Create dataclass-backed loaders for `deploy\config\deploy.yml` plus environment overlays, Terraform `.tfvars` rendering, fabric-cicd config rendering, parameter rendering, and Terraform output JSON loading.

- [x] **Step 3: Implement artifact staging**

Create helpers that stage Lakehouse/Eventhouse/KQL shell item folders, notebook `.Notebook` folders with default Lakehouse metadata, and existing Power BI item directories while excluding local `.pbi` state.

### Task 2: Deployment framework files

**Files:**
- Create: `deploy\config\deploy.yml`
- Create: `deploy\config\environments\dev.yml`
- Create: `deploy\config\environments\test.yml`
- Create: `deploy\config\environments\prod.yml`
- Create: `deploy\terraform\providers.tf`
- Create: `deploy\terraform\variables.tf`
- Create: `deploy\terraform\main.tf`
- Create: `deploy\terraform\outputs.tf`
- Create: `deploy\terraform\environments\dev.tfvars`
- Create: `deploy\terraform\environments\test.tfvars`
- Create: `deploy\terraform\environments\prod.tfvars`
- Create: `deploy\fabric-cicd\config.yml`
- Create: `deploy\fabric-cicd\parameter.yml`

- [x] **Step 1: Add canonical YAML config**

Define workspace, Lakehouse, Eventhouse, KQL, Eventstream, notebook, Power BI, and deployment behavior settings.

- [x] **Step 2: Add Terraform files**

Create workspace/reference logic, role assignments, Lakehouse, Eventhouse, KQL Database, optional Eventstream, and outputs.

- [x] **Step 3: Add fabric-cicd starter config**

Add a dev starter config and parameter file that can be regenerated from Terraform outputs.

### Task 3: Operational wrappers and docs

**Files:**
- Create: `deploy\scripts\generate_configs.py`
- Create: `deploy\scripts\deploy_items.py`
- Create: `deploy\scripts\apply_kql.py`
- Create: `deploy\scripts\validate_deployment.py`
- Create: `deploy\README.md`
- Modify: `.gitignore`

- [x] **Step 1: Add command-line wrappers**

Expose config generation, fabric-cicd deployment, KQL script preparation, and offline validation entry points.

- [x] **Step 2: Add README workflow**

Document the local command sequence and safety defaults.

- [x] **Step 3: Ignore generated state**

Ignore `deploy\.generated`, staged workspace contents, and Terraform state/cache files while keeping `deploy\workspace\.gitkeep`.

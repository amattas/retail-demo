# Capacity Planning

Microsoft Fabric capacity sizing and cost optimization.

## Capacity SKU Recommendations

Microsoft Fabric capacity is measured in Capacity Units (CU). Choose appropriate SKU based on workload:

| Environment | SKU | CUs | vCores | RAM | Use Case |
|-------------|-----|-----|--------|-----|----------|
| **Development/POC** | F2 | 2 | 2 | 8 GB | Small-scale testing, single developer |
| **Development/Testing** | F4 | 4 | 4 | 16 GB | Team development, moderate data volumes |
| **Staging** | F8 | 8 | 8 | 32 GB | Pre-production validation, load testing |
| **Production (Small)** | F16 | 16 | 16 | 64 GB | <5M events/day, <50 concurrent users |
| **Production (Medium)** | F32 | 32 | 32 | 128 GB | 5-20M events/day, 50-200 concurrent users |
| **Production (Large)** | F64+ | 64+ | 64+ | 256+ GB | >20M events/day, >200 concurrent users |

## Workload Sizing Guidelines

**Retail Demo - Expected Resource Usage:**

| Component | Daily Data Volume | Processing Time | Capacity Impact |
|-----------|------------------|-----------------|-----------------|
| **Datagen → Event Hubs** | 1-10M events | Continuous | Minimal (external) |
| **Eventhouse Ingestion** | 1-10M events | Continuous | High (streaming) |
| **Bronze Shortcuts** | 0 GB (references only) | <1 min | Minimal |
| **Silver Transformation** | 500MB - 5GB | 5-10 min per run | Medium (every 5 min) |
| **Gold Aggregation** | 100MB - 1GB | 3-5 min per run | Low (every 15 min) |
| **Power BI DirectLake** | Varies by users | Real-time | Medium (concurrent queries) |

## Sizing Formula

```
Required CUs ≈ (Events per day / 1M) × 2 + (Concurrent users / 50) × 4
```

**Examples:**
- 2M events/day, 10 users: 2×2 + 0.2×4 = **~5 CUs** → F4 or F8
- 10M events/day, 100 users: 10×2 + 2×4 = **~28 CUs** → F32
- 50M events/day, 500 users: 50×2 + 10×4 = **~140 CUs** → F128 or F256

## Auto-Scale Configuration

**When to Enable Auto-Scale:**
- Unpredictable event volumes (promotions, seasonal spikes)
- Variable user concurrency (business hours vs. off-hours)
- Cost optimization (scale down overnight)

**Configuration:**
1. Navigate to Fabric capacity settings in Azure Portal
2. Enable **Auto-scale**
3. Configure:
   - Minimum capacity: Base SKU (e.g., F16)
   - Maximum capacity: Peak SKU (e.g., F64)
   - Scale triggers: CPU/memory thresholds

**Cost Impact:**
- Auto-scale charges per-second at higher SKU when scaled up
- Can reduce costs by 30-50% vs. fixed high SKU
- Monitor scaling patterns to optimize min/max settings

## Capacity Monitoring

**Key Metrics:**
- **CU Utilization**: Target <70% average, <90% peak
- **Throttling Events**: Should be 0 in production
- **Query Queue Time**: Target <5 seconds
- **Pipeline Execution Time**: Should not increase over time

**View Metrics:**
1. Azure Portal → Fabric Capacity → **Monitoring**
2. View:
   - CU consumption over time
   - Top consuming workspaces
   - Throttling incidents
   - Performance trends

**Upgrade Triggers:**
- CU utilization consistently >80%
- Throttling events >5 per day
- Pipeline execution time increases >50%
- User-reported performance issues

## Cost Optimization Tips

1. **Right-size Capacity**: Start with F8, monitor, then adjust
2. **Use Auto-scale**: Enable for variable workloads
3. **Optimize Queries**: Reduce CU consumption via Z-ordering, partitioning
4. **Schedule Maintenance**: Run OPTIMIZE/VACUUM during off-hours
5. **Archive Old Data**: Move inactive data to cold storage (ADLS)
6. **Consolidate Workspaces**: Share capacity across multiple projects
7. **Monitor Idle Capacity**: Pause/scale down non-production during off-hours

## Production Deployment Checklist

Before deploying to production capacity:

- [ ] Load test with expected data volumes
- [ ] Verify auto-scale configuration
- [ ] Set up capacity monitoring and alerts
- [ ] Document baseline CU utilization
- [ ] Configure budget alerts in Azure Cost Management
- [ ] Plan capacity upgrade path for growth
- [ ] Test failover to backup capacity (disaster recovery)

## Region Considerations

- Choose region close to data sources (ADLS, Event Hubs) to minimize latency
- Ensure region supports Real-Time Intelligence features
- Consider multi-region deployment for high availability (advanced)

# AGENTS.md — Real-Time Rules

Spec for alerting and actions. Backed by KQL DB change detection or Eventstream conditions.

Rule Catalog (initial):
- `rule_stockout_detected`: trigger on `stockout_detected` payloads
- `rule_reorder_urgent`: trigger when `reorder_triggered.priority == 'URGENT'`
- `rule_truck_dwell_breach`: detect `truck_arrived` without `truck_departed` beyond threshold
- `rule_zone_overcrowded`: customer_count/dwell exceeds threshold in zone
- `rule_high_value_customer_entered`: join against customer segment list

Actions:
- Teams message (Ops channel)
- Email/SMS
- Webhook to ticketing (e.g., ServiceNow/Jira)
- Power Automate flow (optional)

SLOs:
- Detection-to-delivery < 30 seconds for urgent rules


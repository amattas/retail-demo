# KQL querysets

Each `*.kql` file in this directory becomes one tab in the deployed
`retail_querysets.KQLQueryset` item. Artifact staging binds the queryset to the
target KQL database and rewrites the database item identifier.

The current queries cover sales, inventory, customer journey, logistics, and
marketing investigations.

`q_stockout_detections_by_store.kql` intentionally reports recent detection
events rather than "open" stockouts because the event contract has no stockout
resolution event. Zone dwell queries weight minute-level averages by customer
count, and truck dwell queries use completed, nonfuture departures.

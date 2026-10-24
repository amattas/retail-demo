# Sample retail questions: Data Agent & Ontology MCP

Use these questions to evaluate the Fabric Data Agent over `retail_model`
and Ontology MCP over the retail business graph. They are test prompts, not
verified live answers. For a presentation, use the
[CIO walkthrough](cio-walkthrough.md).

Retest each question with the current profile, identity, and data window.
Some multi-hop ontology queries have failed. Avoid `receipt_created`
sales-velocity queries, cross-entity event traversal, `store N` name
resolution, and open-ended 360 views on the ontology. Prefer event counts
grouped by `store_id` or `dc_id`, named-customer lookups, and relationships
to ML predictions. Use the Data Agent or KQL for sales velocity.

## Use case summary

The demo models a synthetic retailer selling general merchandise and
groceries through stores and online channels. Business users can ask about
sales, inventory, customers, and operations without writing SQL, KQL, or DAX.

The Data Agent uses semantic-model measures for metrics, trends, and rankings.
Ontology MCP exposes relationships among entities such as Store, Customer,
Product, Receipt, and ChurnPrediction. The labels below identify the intended
data source for each question; they do not guarantee that a service can answer.

| Label | Source | Example content |
| --- | --- | --- |
| DA | Data Agent over Lakehouse Silver/Gold through the semantic model | Sales, margin, inventory, logistics, traffic, and marketing |
| RT | Eventhouse KQL event tables and materialized views | Recent sales velocity, product sales, zone dwell, tender mix, and anomalies |
| ONT | Selected retail ontology graph | Customer→Receipt→Product, Store←Truck←DC, Promotion→Customer; verify current bindings |
| ML | Predictive Gold tables | Churn, segmentation, forecasts, stockout risk, market basket, elasticity, pricing, journeys, and delivery dwell |

Start with sections 1-15 to test individual capabilities, then try the
combined questions in section 16. Replace relative periods such as "last
30 days," "this quarter," and "yesterday" with dates present in your workspace.

## 1. Executive & sales performance (DA)

1. What were total net sales, gross profit, and gross margin % company-wide last quarter, and how did that compare to the prior quarter?
2. Rank regions by net sales and units for the trailing 30 days, with each region's share of total sales.
3. Which top 10 stores drove the most net sales last month, and how does each compare to the same month last year (YoY %)?
4. Which store volume class (flagship, high_volume, standard, low_volume, kiosk) has the highest sales per store and average basket?
5. What is total omnichannel revenue (in-store and online), and what percentage is digital? How has the channel mix changed since last year?
6. Show month-over-month net sales trend for the last 6 months with the largest single-month swing called out.
7. What are sales per customer and units per receipt company-wide, and which regions have above-average basket sizes?
8. Which product departments have the largest sales gains and declines versus last year?

## 2. Merchandising & product (DA / ONT)

9. What are the top 15 products by revenue last month, and what is each product's gross margin %? (DA)
10. For the Electronics department, show revenue and units by Category and Subcategory, drilling from Department → Category → Subcategory. (DA)
11. Which products have the highest return volume? Are their returns spread across stores or concentrated in a few? (DA)
12. Using the graph, list the distinct products a specific high-value customer has purchased through in-store receipts and online orders. (ONT)
13. Through the ontology, find which stores sold a given product and which customers bought it. Show the Store → Receipt → Product → Customer path. (ONT)
14. Which products have the highest MSRP-to-cost spread but the lowest sell-through? (DA)

## 3. Omnichannel & fulfillment (DA)

15. Compare average order value and units per order between in-store and online channels for the last 30 days.
16. How is the fulfillment mix trending (Ship-from-DC vs Ship-from-Store vs BOPIS) month over month, and which mode is growing fastest?
17. Which stores are absorbing the most Ship-from-Store and BOPIS volume, and is that concentrated in specific regions?
18. What is online revenue year-to-date vs prior year, and what is the digital share of total omnichannel revenue?
19. What is the payment tender mix (by method) and how does declined-payment rate vary by channel?

## 4. Inventory, replenishment & supply chain (DA / ONT)

20. What is current total units on hand across stores vs DCs, and the network inventory-to-sales ratio?
21. Which stores have the most open high-priority reorders right now, and what is the total reorder fill gap?
22. Rank DCs by units on hand and SKU count; which DC is carrying the most inventory relative to the stores it serves? (ONT)
23. Through the graph, trace the replenishment path for a flagged store: which DC and which trucks service it (Store ← Truck ← DistributionCenter)? (ONT)
24. What is average truck dwell time, and how many truck-dwell breach days occurred last month by site?
25. Which stores show the highest stockout event counts, and does that correlate with their reorder fill gap?

## 5. Stores, traffic & in-store experience (DA / RT)

26. Which stores had the highest foot traffic but the lowest conversion (receipts per visitor) last week? (DA)
27. What is average customer dwell time by store zone (Entrance, Electronics, Grocery, Clothing, Checkout), and where do customers stay longest? (DA)
28. Show sales velocity (sales per minute) by store for the current hour. (RT)
29. What were the top-selling products in the last 15 minutes across all stores? (RT)
30. Which checkout zones have the longest dwell in the last few minutes, possibly indicating queues? (RT)

## 6. Marketing & promotions (DA / ONT)

31. What is total marketing spend and impressions by campaign last month, and the resulting CPM?
32. Which promotions accounted for the most discount dollars, and how many receipts used each one? (DA)
33. For a specific promo code, use the graph to find which stores ran it and which customers redeemed it (Promotion → Store / Promotion → Customer). (ONT)
34. What is the promotion discount as a percentage of gross sales, and which department has the largest margin reduction from promotions?

## 7. Customer segmentation (RFM, ML)

35. How many customers fall into each segment (Champions, Loyal Customers, Potential Loyalists, New Customers, At Risk, Hibernating, Lost), and what is each segment's average lifetime value?
36. What share of total monetary value comes from Champions and Loyal Customers, and how many customers does that represent?
37. Which segment has gone longest since its last purchase, and what are its average purchase frequency and order value?
38. Compare average order value across segments. Do Champions spend more per order, buy more often, or both?

## 8. Churn prediction (ML / ONT)

39. How many customers are predicted to churn, what is the predicted churn rate %, and what is the average churn probability across the base?
40. List the high-risk customers (Risk Category = High) with the largest historical monetary value.
41. Compare predicted and observed churn to check the model's precision and recall.
42. Using the graph, show the RFM segment and recent purchase history of customers with the highest churn risk (Customer → ChurnPrediction + Customer → Segment). (ONT)
43. Which region or store has the highest concentration of high-risk, high-value customers?

## 9. Demand forecasting (ML)

44. What are total forecasted units for the next horizon by store, and which stores have the widest confidence interval (most uncertain demand)?
45. Which product/store combinations have the highest forecast mean absolute percentage error (MAPE)?
46. For a given high-volume store, show forecasted units vs lower/upper bound for its top products.
47. Where does forecasted demand most exceed current on-hand inventory, suggesting a need for replenishment?

## 10. Stockout risk (ML / ONT)

48. Which store/product positions are at high stockout risk, ranked by stockout probability, and how many days of inventory remain?
49. How many SKUs are flagged high-risk, and what is the average days-of-inventory and daily demand velocity across them?
50. Compare stockout-risk predictions with open reorders. Which high-risk SKUs lack a pending reorder? (ONT)
51. For the highest-velocity products, which stores have the thinnest cover (lowest days of inventory)?

## 11. Market basket / recommendations (ML)

52. Which product pairs have the highest association lift and confidence, making them candidates for cross-merchandising or bundling?
53. For a given anchor product, what are the top recommended add-on products and the confidence of each association?
54. Which high-lift product associations cross department boundaries, such as a grocery item associated with an electronics accessory?

## 12. Price elasticity & dynamic pricing (ML)

55. Which products are Highly Elastic versus Inelastic, and what do those estimates suggest about price sensitivity?
56. Show the dynamic-pricing recommendations with the largest gap between current price and elasticity-optimal price, with the model's confidence on each.
57. For elastic products, what revenue lift does the recommended price imply, and which constraints are binding?
58. Which categories are most price-sensitive, and what does that suggest for promotion planning?

## 13. Customer journey & behavior (ML / RT)

59. What are the most common in-store zone-transition paths, and where do journeys most often end without reaching Checkout (drop-off)?
60. Which zones have the highest dwell, and where do customer journeys most often end?
61. Which customers (by BLE id) have moved through at least three zones in the current live session without a purchase? (RT)

## 14. Delivery / logistics prediction (ML)

62. What are the predicted truck-unload dwell times and prediction intervals for upcoming deliveries? Which sites are at risk of long dwell?
63. Which lanes or sites consistently exceed predicted dwell, suggesting a structural bottleneck?

## 15. Real-time anomaly detection (RT)

64. Are there any sales or inventory anomalies detected in the last hour (sudden velocity spikes/drops)?
65. Which stores are showing abnormal stockout-detection or reorder-trigger rates right now versus their normal baseline?

## 16. Combined metrics, relationships, and predictions

These questions require multiple data sources or model outputs. Treat them
as evaluation targets, not as a supported sequence of autonomous actions.

66. Identify Champions and Loyal customers flagged as high churn risk, list their preferred departments, and suggest products for a win-back offer using segments, churn predictions, purchase history, and market-basket associations. (ONT, ML)
67. What is the total historical revenue associated with high-churn-risk customers? Keep historical spend separate from estimates of future revenue at risk. (DA, ML)
68. For products at high stockout risk in high-traffic stores, estimate at-risk daily sales using forecasted demand velocity. (ML)
69. For the top 10 promoted products with the largest margin reductions, compare price elasticity. Which discounts applied to products estimated to be inelastic? (DA, ML)
70. Where does next-period forecasted demand exceed combined store and DC inventory, and is a truck already scheduled for that store? (ML, ONT)
71. Compare zone dwell and store conversion. Do stores with longer Checkout dwell have lower conversion? (ML, DA)
72. For each store with sales below its regional median, list its top stockout risk, churn-risk customers, and recommended cross-sell in priority order. (DA, ONT, ML)

## Notes for the tester

ML questions in sections 7-14 and 16 require predictive Gold tables. The
`ml-required` pipeline gates six outputs: `churn_predictions`,
`customer_segments`, `demand_forecast`, `stockout_risk`,
`product_recommendations`, and `price_elasticity`. Post-Reporting
`ml-optional` produces `journey_patterns` and `dwell_predictions`;
`ml-experimental` produces `pricing_recommendations`. Confirm the relevant
pipelines ran and their tables are populated and bound before testing.

Real-time questions require fresh Eventhouse event tables and materialized
views. Run the streaming notebook and check ingestion freshness first.

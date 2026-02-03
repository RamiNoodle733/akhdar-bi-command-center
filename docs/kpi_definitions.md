# KPI Definitions

This document defines the official KPI calculations used in the Akhdar BI Command Center. All dashboards and reports should reference these definitions for consistency.

## Revenue KPIs

### Gross Product Sales
- **Definition:** Total revenue from products before any deductions
- **Formula:** `SUM(lineitem_price × lineitem_quantity)`
- **Source:** Calculated from `staging.stg_order_lines`, stored in `warehouse.fact_order.gross_product_sales`
- **Grain:** Order level (1 value per order)
- **Notes:** Does not include shipping or taxes

### Order Discounts Total
- **Definition:** Total discount amount applied at order level
- **Formula:** `SUM(discount_amount)` from orders
- **Source:** `warehouse.fact_order.order_discount_amount`
- **Grain:** Order level
- **Notes:** Shopify applies discounts at order level; allocated proportionally to lines for product profitability

### Line Discounts Total  
- **Definition:** Total discount amount applied at line item level
- **Formula:** `SUM(lineitem_discount)`
- **Source:** `warehouse.fact_order_line.line_discount`
- **Notes:** Currently $0 in our data; order-level discounts are more common

### Refunds Total
- **Definition:** Total amount refunded to customers
- **Formula:** `SUM(refunded_amount)`
- **Source:** `warehouse.fact_order.refunded_amount`
- **Notes:** Currently $0; column exists for future refunds

### Net Sales
- **Definition:** Revenue after discounts and refunds (excluding shipping/tax)
- **Formula:** `Subtotal - Refunded Amount`
- **Source:** `warehouse.fact_order.net_sales`
- **Validation:** Should equal `Gross Product Sales - Discounts Total - Refunds Total`

### Average Order Value (AOV)
- **Definition:** Average revenue per order
- **Formula:** `Net Sales / COUNT(DISTINCT order_id)`
- **Source:** `marts.kpi_revenue_totals.overall_aov`
- **Notes:** Uses net sales (after discounts/refunds), excludes shipping

### Refund Rate
- **Definition:** Percentage of gross revenue that was refunded
- **Formula:** `(Refunds Total / Gross Product Sales) × 100`
- **Source:** `marts.kpi_revenue_totals.refund_rate_percent`

---

## Product Profitability KPIs

### Gross Line Revenue
- **Definition:** Revenue from a single line item before deductions
- **Formula:** `unit_price × quantity`
- **Source:** `warehouse.fact_order_line.gross_line_revenue`

### Allocated Order Discount
- **Definition:** Portion of order discount attributed to this line item
- **Formula:** `(line_gross / order_gross) × order_discount_amount`
- **Source:** `warehouse.fact_order_line.allocated_order_discount`
- **Notes:** Enables accurate product-level profitability when order discounts exist

### Net Line Revenue
- **Definition:** Revenue from a line item after all discounts
- **Formula:** `Gross Line Revenue - Line Discount - Allocated Order Discount`
- **Source:** `warehouse.fact_order_line.net_line_revenue`

### Estimated COGS (Known)
- **Definition:** Cost of goods sold based on recipe ingredients with known costs
- **Formula:** Sum of `(ingredient_ml × cost_per_ml)` for all ingredients with `has_known_cost = true`, plus packaging costs
- **Source:** `warehouse.fact_order_line.estimated_cogs`
- **Components:**
  - Jojoba Oil: $0.03/ml
  - Sandalwood (10% diluted): $1.15/ml
  - Lavender EO: $0.65/ml
  - Patchouli EO: $0.35/ml
  - Clove Bud EO: $0.21/ml
  - Sweet Orange EO: $0.17/ml
  - Labdanum Resinoid: $1.57/ml
  - Roller Bottle (5ml): $0.34/piece
  - Label: $0.01/piece

### Has Missing Cost Flag
- **Definition:** Indicates if COGS is incomplete due to unknown ingredient costs
- **Source:** `warehouse.fact_order_line.has_missing_cost`
- **Unknown Cost Ingredients:**
  - Rose Givco 217 (aromachemical)
  - Galaxolide 50% in IPM (musk)
  - Oud Synth 10760 E (oud)
  - Sandalore (sandalwood)

### Gross Margin (Dollars)
- **Definition:** Profit per unit after COGS
- **Formula:** `(Net Line Revenue / Quantity) - Estimated COGS`
- **Source:** `warehouse.fact_order_line.gross_margin`
- **⚠️ Caveat:** Understated when `has_missing_cost = true`

### Gross Margin Percent
- **Definition:** Margin as percentage of net revenue
- **Formula:** `(Gross Margin / (Net Line Revenue / Quantity)) × 100`
- **Source:** `warehouse.fact_order_line.margin_percent`
- **⚠️ Caveat:** Overstated when `has_missing_cost = true`

---

## Customer KPIs

### Customer Segment
- **Definition:** Classification based on purchase history
- **Values:**
  - `prospect` - Has account but 0 orders
  - `new` - 1 order (first purchase)
  - `returning` - 2+ orders
- **Source:** `warehouse.dim_customer.customer_segment`
- **Logic:** Based on `first_order_date` comparison to order date

### New vs Returning (by Order)
- **Definition:** Breakdown of orders by customer type
- **Formula:** 
  - New order: `order_date = customer.first_order_date`
  - Returning order: `order_date > customer.first_order_date`
- **Source:** `marts.kpi_new_vs_returning`

### Repeat Purchase Rate
- **Definition:** Percentage of customers who made 2+ purchases
- **Formula:** `Customers with 2+ orders / Total Customers with Orders × 100`
- **Source:** `marts.kpi_repeat_purchase_rate.repeat_purchase_rate_percent`

### Average Lifetime Value (LTV)
- **Definition:** Average total spend per customer
- **Formula:** `AVG(total_spent)` across all customers with orders
- **Source:** `marts.kpi_repeat_purchase_rate.avg_lifetime_value`

---

## Marketing KPIs

### Amount Spent
- **Definition:** Total ad spend in USD
- **Source:** `warehouse.fact_marketing_spend.amount_spent`
- **Platform:** Meta Ads Manager

### Reach
- **Definition:** Unique users who saw the ad
- **Source:** `warehouse.fact_marketing_spend.reach`

### Impressions
- **Definition:** Total times the ad was displayed
- **Source:** `warehouse.fact_marketing_spend.impressions`

### CPM (Cost per 1,000 Impressions)
- **Definition:** Cost to reach 1,000 impressions
- **Formula:** `(Amount Spent / Impressions) × 1000`
- **Source:** `warehouse.fact_marketing_spend.cpm`

### Link Clicks
- **Definition:** Clicks on the ad link
- **Source:** `warehouse.fact_marketing_spend.link_clicks`

### CPC (Cost per Click)
- **Definition:** Cost per link click
- **Formula:** `Amount Spent / Link Clicks`
- **Source:** `warehouse.fact_marketing_spend.cpc`

### CTR (Click-through Rate)
- **Definition:** Percentage of impressions that resulted in clicks
- **Formula:** `(Link Clicks / Impressions) × 100`
- **Source:** `warehouse.fact_marketing_spend.ctr`

### ROAS (Return on Ad Spend)
- **Definition:** Revenue generated per dollar spent on ads
- **Formula:** `Attributed Revenue / Amount Spent`
- **Source:** Not yet implemented (requires attribution model)
- **Notes:** Requires order-to-campaign attribution which is not available in current data

---

## SEO KPIs (Google Search Console)

### Organic Clicks
- **Definition:** Clicks from Google search results
- **Source:** `warehouse.fact_gsc_daily.clicks`

### Impressions
- **Definition:** Times site appeared in search results
- **Source:** `warehouse.fact_gsc_daily.impressions`

### CTR (Search)
- **Definition:** Click-through rate from search results
- **Formula:** `Clicks / Impressions`
- **Source:** `warehouse.fact_gsc_daily.ctr`

### Average Position
- **Definition:** Average ranking position in search results
- **Source:** `warehouse.fact_gsc_daily.avg_position`
- **Notes:** Lower is better (1 = top result)

---

## Data Quality Validations

These checks run automatically in `tests/test_data_quality.py`:

| Validation | Formula | Tolerance |
|------------|---------|-----------|
| Subtotal Check | `Gross Product Sales - Discount = Subtotal` | ±$0.01 |
| Total Check | `Subtotal + Shipping + Tax = Total` | ±$0.01 |
| Line Sum Check | `SUM(line_gross) = order_gross` | ±$0.01 |
| Non-negative | All amounts ≥ 0 | Exact |
| No Duplicates | 1 row per order in fact_order | Exact |

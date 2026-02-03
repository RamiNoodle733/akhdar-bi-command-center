# Data Dictionary

This document describes all tables and columns in the Akhdar BI Command Center data warehouse.

---

## Schema Overview

| Schema | Purpose |
|--------|---------|
| `raw` | Source data as-is from CSV imports |
| `staging` | Cleaned, typed data with validations |
| `warehouse` | Star schema (facts + dimensions) |
| `marts` | KPI views for reporting |
| `powerbi_export` | PII-safe views for Power BI |

---

## Dimension Tables

### warehouse.dim_date

Calendar dimension for time-based analysis.

| Column | Type | Description |
|--------|------|-------------|
| date_key | INTEGER | Primary key (YYYYMMDD format) |
| full_date | DATE | Calendar date |
| year | INTEGER | Calendar year |
| quarter | INTEGER | Quarter (1-4) |
| month | INTEGER | Month (1-12) |
| month_name | VARCHAR | Full month name |
| week_of_year | INTEGER | Week number (1-53) |
| day_of_month | INTEGER | Day (1-31) |
| day_of_week | INTEGER | Day of week (0=Sunday) |
| day_name | VARCHAR | Full day name |
| is_weekend | BOOLEAN | Saturday or Sunday |
| is_holiday | BOOLEAN | Holiday flag (manual) |
| fiscal_year | INTEGER | Fiscal year |
| fiscal_quarter | INTEGER | Fiscal quarter |

---

### warehouse.dim_product

Product dimension with SKU mappings.

| Column | Type | Description |
|--------|------|-------------|
| product_key | SERIAL | Surrogate key |
| internal_sku | VARCHAR | Internal SKU code (e.g., SHELL_V2_5ML) |
| product_handle | VARCHAR | Shopify URL handle |
| product_title | VARCHAR | Full product name |
| size_ml | INTEGER | Product size in milliliters |
| recipe_id | VARCHAR | Link to recipe/BOM |
| product_category | VARCHAR | Product category |
| vendor | VARCHAR | Product vendor |
| variant_price | NUMERIC | List price |
| is_active | BOOLEAN | Currently for sale |

---

### warehouse.dim_customer

Customer dimension (PII-safe).

| Column | Type | Description |
|--------|------|-------------|
| customer_key | SERIAL | Surrogate key |
| customer_id_hash | VARCHAR | SHA256 hash of email (PII-safe) |
| customer_id | BIGINT | Shopify customer ID |
| city | VARCHAR | Customer city |
| province | VARCHAR | State/province |
| province_code | VARCHAR | State code (e.g., TX) |
| country | VARCHAR | Country name |
| country_code | VARCHAR | Country code (e.g., US) |
| accepts_email_marketing | BOOLEAN | Email opt-in |
| accepts_sms_marketing | BOOLEAN | SMS opt-in |
| first_order_date | DATE | Date of first purchase |
| total_orders | INTEGER | Lifetime order count |
| total_spent | NUMERIC | Lifetime spend |
| customer_segment | VARCHAR | new / returning / prospect |

**Note:** Email, name, phone, address are intentionally excluded for PII protection.

---

### warehouse.dim_channel

Sales channel dimension.

| Column | Type | Description |
|--------|------|-------------|
| channel_key | SERIAL | Surrogate key |
| channel_code | VARCHAR | Channel identifier |
| channel_name | VARCHAR | Display name |
| channel_type | VARCHAR | web / pos / api |
| is_online | BOOLEAN | Online channel flag |

---

### warehouse.dim_shipping_method

Shipping method dimension.

| Column | Type | Description |
|--------|------|-------------|
| shipping_method_key | SERIAL | Surrogate key |
| shipping_method_code | VARCHAR | Method identifier |
| shipping_method_name | VARCHAR | Display name |
| is_local_delivery | BOOLEAN | Local delivery flag |

---

### warehouse.dim_material

Raw materials and packaging dimension.

| Column | Type | Description |
|--------|------|-------------|
| material_key | SERIAL | Surrogate key |
| material_id | VARCHAR | Material code |
| material_name | VARCHAR | Material name |
| ingredient_match | VARCHAR | Name as appears in recipes |
| category | VARCHAR | Packaging / Carrier Oil / Essential Oil / Aromachemical |
| unit | VARCHAR | Unit of measure (ml, piece) |
| cost_per_unit | NUMERIC | Cost per unit (for packaging) |
| cost_per_ml | NUMERIC | Cost per milliliter (for liquids) |
| has_known_cost | BOOLEAN | Cost is known and accurate |
| supplier | VARCHAR | Supplier name |

---

## Fact Tables

### warehouse.fact_order

Order-level fact table (1 row per order).

| Column | Type | Description |
|--------|------|-------------|
| order_key | SERIAL | Surrogate key |
| order_id | BIGINT | Shopify order ID |
| order_number | VARCHAR | Display order number (#Akhdar1001) |
| order_date_key | INTEGER | FK to dim_date |
| customer_key | INTEGER | FK to dim_customer |
| channel_key | INTEGER | FK to dim_channel |
| shipping_method_key | INTEGER | FK to dim_shipping_method |
| gross_product_sales | NUMERIC | Sum of (price × qty) |
| order_discount_amount | NUMERIC | Order-level discount |
| subtotal | NUMERIC | Gross - discount |
| shipping_amount | NUMERIC | Shipping charged |
| tax_amount | NUMERIC | Tax charged |
| total_amount | NUMERIC | Customer total |
| refunded_amount | NUMERIC | Amount refunded |
| net_sales | NUMERIC | Subtotal - refunds |
| line_item_count | INTEGER | Number of line items |
| unit_count | INTEGER | Total units ordered |
| financial_status | VARCHAR | paid / pending / refunded |
| fulfillment_status | VARCHAR | fulfilled / unfulfilled |
| risk_level | VARCHAR | Low / Medium / High |
| created_at | TIMESTAMP | Order creation time |
| paid_at | TIMESTAMP | Payment time |
| fulfilled_at | TIMESTAMP | Fulfillment time |

**Important:** Order-level amounts (subtotal, discount, refund) are NOT duplicated across line items. Sum only from this table for order KPIs.

---

### warehouse.fact_order_line

Line item fact table (1 row per product in order).

| Column | Type | Description |
|--------|------|-------------|
| order_line_key | SERIAL | Surrogate key |
| order_key | INTEGER | FK to fact_order |
| order_id | BIGINT | Shopify order ID |
| line_number | INTEGER | Line sequence (1, 2, 3...) |
| product_key | INTEGER | FK to dim_product |
| order_date_key | INTEGER | FK to dim_date |
| quantity | INTEGER | Units ordered |
| unit_price | NUMERIC | Price per unit |
| gross_line_revenue | NUMERIC | price × quantity |
| line_discount | NUMERIC | Direct line discount |
| allocated_order_discount | NUMERIC | Proportional order discount |
| net_line_revenue | NUMERIC | Gross - discounts |
| estimated_cogs | NUMERIC | Cost per unit |
| has_missing_cost | BOOLEAN | COGS incomplete flag |
| gross_margin | NUMERIC | Revenue - COGS per unit |
| margin_percent | NUMERIC | Margin as % of revenue |

---

### warehouse.fact_cogs_estimate

COGS detail by ingredient per order line.

| Column | Type | Description |
|--------|------|-------------|
| cogs_key | SERIAL | Surrogate key |
| order_line_key | INTEGER | FK to fact_order_line |
| product_key | INTEGER | FK to dim_product |
| material_key | INTEGER | FK to dim_material |
| ingredient_name | VARCHAR | Ingredient name |
| amount_ml | NUMERIC | Amount used (ml) |
| cost_per_ml | NUMERIC | Unit cost |
| line_cost | NUMERIC | amount × cost |
| has_known_cost | BOOLEAN | Cost is known |

---

### warehouse.fact_marketing_spend

Marketing campaign performance (Meta Ads).

| Column | Type | Description |
|--------|------|-------------|
| marketing_key | SERIAL | Surrogate key |
| campaign_name | VARCHAR | Campaign name |
| platform | VARCHAR | Ad platform (meta) |
| reach | INTEGER | Unique users reached |
| impressions | INTEGER | Total impressions |
| amount_spent | NUMERIC | USD spent |
| link_clicks | INTEGER | Link clicks |
| landing_page_views | INTEGER | Landing page views |
| cpm | NUMERIC | Cost per 1000 impressions |
| cpc | NUMERIC | Cost per click |
| ctr | NUMERIC | Click-through rate |

---

### warehouse.fact_gsc_daily

Google Search Console daily metrics.

| Column | Type | Description |
|--------|------|-------------|
| gsc_daily_key | SERIAL | Surrogate key |
| date_key | INTEGER | FK to dim_date |
| clicks | INTEGER | Search clicks |
| impressions | INTEGER | Search impressions |
| ctr | NUMERIC | Click-through rate |
| avg_position | NUMERIC | Average search position |

---

## Source File Mappings

| Source File | Target Table | Notes |
|-------------|--------------|-------|
| orders_export_1.csv | raw.orders | Contains PII |
| products_export_1.csv | raw.products | |
| customers_export.csv | raw.customers | Contains PII |
| discounts_export_1.csv | raw.discounts | |
| product_sku_map.csv | raw.product_sku_map | Reference data |
| material_costs.csv | raw.material_costs | Reference data |
| recipes.csv | raw.recipes | BOM reference |
| *Meta ads CSV* | raw.meta_ads | Optional |
| Chart.csv (GSC) | raw.gsc_daily | Optional |

---

## Data Type Conventions

| Type | PostgreSQL | Description |
|------|------------|-------------|
| Keys | SERIAL / BIGINT | Surrogate or natural keys |
| Amounts | NUMERIC(10,2) | Currency amounts |
| Rates | NUMERIC(10,4) | Percentages, ratios |
| Dates | DATE | Calendar dates |
| Timestamps | TIMESTAMP WITH TIME ZONE | Date/time with timezone |
| Flags | BOOLEAN | True/false indicators |
| Text | VARCHAR(n) | Variable-length strings |

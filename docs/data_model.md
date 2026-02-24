# Data Model Documentation

## Overview

This data warehouse follows a **medallion architecture** with three layers:
- **Bronze**: Raw data from source systems
- **Silver**: Cleaned, validated, and deduplicated data
- **Gold**: Business-ready aggregated views

---

## Bronze Layer (Raw)

### `raw.pos_sales`
Raw sales transactions from the POS system.

| Column | Type | Description |
|--------|------|-------------|
| sale_id | VARCHAR(50) | Primary key |
| sale_date | TIMESTAMP | Transaction date/time |
| store_id | VARCHAR(20) | Store identifier |
| product_id | VARCHAR(30) | Product identifier |
| quantity | INTEGER | Units sold |
| unit_price | DECIMAL(10,2) | Price per unit |
| discount | DECIMAL(10,2) | Discount applied |
| total_amount | DECIMAL(10,2) | Final amount |
| payment_method | VARCHAR(20) | Payment type |
| customer_id | VARCHAR(30) | Customer (if known) |

### `raw.erp_inventory`
Inventory data from ERP system.

### `raw.crm_customers`
Customer data from CRM system.

### `raw.products`
Product catalog from source systems.

---

## Silver Layer (Cleaned)

Data in this layer has been:
- Deduplicated
- Nulls handled with COALESCE
- Strings trimmed and standardized
- Emails lowercased
- Phone numbers normalized

---

## Gold Layer (Aggregated)

### `gold.daily_sales_summary`
Daily sales KPIs by store.

### `gold.customer_360`
Complete customer profile with lifetime value.

### `gold.product_performance`
Product revenue and profit analysis.

### `gold.store_performance`
Store-level metrics.

---

## SCD Type 2 Tables

- `gold.dim_customer_scd` - Customer history with address/segment changes
- `gold.dim_product_scd` - Product history with price/category changes

---

## ETL Metadata

- `etl.watermarks` - Tracks incremental load progress

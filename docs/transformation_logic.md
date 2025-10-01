# ETL Transformation Logic

## 1. Sales Summary by Product
**Purpose:** Aggregate sales metrics by product for analysis

**Source Tables:**
- sales_csv

**Transformations:**
- Group by productkey
- Sum total sales amount
- Sum total quantity sold
- Count number of orders
- Calculate average unit price

**Output:** sales_summary_by_product

---

## 2. Reseller Geographic Analysis
**Purpose:** Enrich reseller data with geographic information

**Source Tables:**
- reseller_csv
- region_csv

**Transformations:**
- Join resellers with regions
- Add regional grouping (North America, Europe, Other)
- Standardize geographic fields

**Output:** reseller_geographic

---

## 3. Employee Sales Performance
**Purpose:** Compare actual sales vs targets by employee

**Source Tables:**
- salesperson_csv
- sales_csv
- targets_csv

**Transformations:**
- Aggregate actual sales by employee
- Join with employee details
- Join with targets
- Calculate achievement percentage
- Calculate variance (actual - target)

**Output:** employee_performance
# Data Validation Report for 'new_order.csv'

## Validation Status: **Failed**

**Overall Analysis:** The dataset exhibits multiple high-impact issues, notably nulls in 'OrderID' and type mismatches in 'Quantity', which prevent seamless integration. Schema drift is minimal, but data quality violations are significant, requiring data cleansing and validation enhancements. An upsert strategy is recommended given the primary key issues.

### At a Glance
| Metric | Value |
| :--- | :--- |
| Target Table (Inferred) | `customer_orders` |
| High Severity Issues | 2 |
| Medium Severity Issues | 2 |
| Low Severity Issues | 0 |
| Total Rows Checked | 5 |
| Processed At | 2025-10-31T10:33:02.223340+00:00 |

## 1. Schema Mismatch Analysis
**Analysis:** File has 4 missing columns and 4 extra columns; two columns are semantically mapped. 'OrderID', 'OrderDate', and 'DiscountCode' are correctly named, but 'cust' and 'qty' should map to 'CustomerID' and 'Quantity' respectively.

#### Columns Missing from File (Required by Table):
- `OrderID`
- `CustomerID`
- `OrderDate`
- `Quantity`
- `DiscountCode`

#### Extra Columns Found in File (Not in Table):
- `cust`
- `qty`
- `CustomerEmail`
- `ShippingMethod`

#### Suggested Naming Mappings:
- Map `cust` (file) to `CustomerID` (table)
- Map `qty` (file) to `Quantity` (table)

#### Recommendations:
- `Map 'cust' to 'CustomerID' to ensure data aligns with the target schema.`
- `Map 'qty' to 'Quantity'. Validate 'qty' values to ensure all are numeric integers; convert or clean data as needed.`
- `Add 'OrderID', 'OrderDate', and 'DiscountCode' columns to the source file if data is missing or populate default values where appropriate.`
- `Review 'CustomerEmail' and 'ShippingMethod' columns to determine if they should be integrated into the schema or discarded.`
- `Implement data type validations for 'Quantity' to enforce integer values and parse 'qty' accordingly.`

## 2. Data Type Violations

- **Column: `Quantity`**
  - **Severity:** High
  - **Expected Type:** `INTEGER`
  - **Found Type:** `object`
  - **Invalid Samples:** `['one']`
  - **Suggestion:** Convert the 'Quantity' column values to numeric integers, coercing invalid entries to NaN, then fill or handle NaNs as appropriate.
  - **Cleaning Code:** `df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)`

## 3. Data Quality Violations

- **Column: `OrderID`**
  - **Check:** `not_null_violation`
  - **Severity:** High
  - **Count:** 1
  - **Details:** Column 'OrderID' contains null or empty string in at least one row (index 3), which violates non-null constraint.

## 4. Root Cause Analysis
**Primary Cause:** Data inconsistency and incomplete data validation procedures caused missing critical identifier ('OrderID') and incorrect data types for 'Quantity'.

**Secondary Causes:**
- `Lack of enforced data validation rules during data ingestion allowed nulls in non-nullable fields.`
- `Type mismatch in 'Quantity' column due to inconsistent data entries ('one' vs numeric strings).`
- `Additional columns like 'CustomerEmail' are recent schema additions and not yet fully validated.`

**Recommendations:**
- `Implement rigorous validation rules during ETL to prevent nulls in non-nullable fields.`
- `Standardize data types with prior data cleaning, especially converting 'Quantity' to integers.`
- `Update dynamic validation rules to cover new columns like 'CustomerEmail'.`

## 5. Suggested Load Strategy
- **Strategy:** `UPSERT`
- **Reasoning:** Primary key violation detected: 'OrderID' contains duplicate or null values, indicating existing records may need updating rather than appending duplicates.
- **Recommendation:** Implement an upsert strategy on 'OrderID' to update existing records with new data and insert new entries, ensuring data integrity and avoiding duplicates.

## 6. Schema Drift
**Analysis Summary:** No significant schema drift detected; the main columns remain consistent. The 'CustomerEmail' column is additional in the current file, introduced recently.

**New Columns Detected:**
- `CustomerEmail`

**Removed Columns Detected:**
- None

## 7. Inferred Validation Rules
| Column | Rule Type | Details | Inferred From |
| :--- | :--- | :--- | :--- |
| `OrderID` | `format_check` | Sample values follow a pattern starting with 'ORD' followed by digits: 'ORD\d+'. | `['ORD1004', 'ORD1005']` |
| `OrderID` | `null_check` | Null count is 1, indicating this column should not be null. | `['ORD1004']` |
| `cust` | `enum_check` | Customer IDs follow pattern 'CUST' followed by three digits: 'CUST\d{3}'. | `['CUST003', 'CUST004']` |
| `OrderDate` | `format_check` | Dates are in ISO format: YYYY-MM-DD. | `['2025-10-24', '2025-10-25']` |
| `OrderDate` | `range_check` | Dates should be within 2025-01-01 and 2099-12-31. | `['2025-10-24', '2099-12-31']` |
| `qty` | `format_check` | Values are numeric strings; non-numeric ('five') suggests need for numeric validation. | `['10', 'five']` |
| `qty` | `numeric_check` | All valid entries are numeric strings, should convert to integers. | `['10', '5', '3', '2']` |
| `Price` | `range_check` | Prices are positive floats, range likely from 0 to around 75. | `[25.0, 12.5, 20.0, 50.0, 75.0]` |
| `ShippingMethod` | `enum_check` | Categorical values from predefined options. | `['Standard', 'Express', 'Priority']` |
| `CustomerEmail` | `format_check` | Most follow email regex patterns; 'bad-email' requires validation. | `['user@example.com', 'bad-email', 'test@test.com', 'future@date.com']` |
| `CustomerEmail` | `null_check` | Null values are acceptable but should be validated if present. | `[None]` |
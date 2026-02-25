Scenario Question: Airbnb — Clean Listing Amenities

## 🗂️ Scenario

You are working with raw **Airbnb listing data** ingested from multiple sources.  
Each listing contains property details and a **nested list of amenities**.  
The goal is to **clean, normalize, and store** this data for downstream analysis.

The data is available as a JSON file (`listings.json`) in the **Bronze layer**, which now needs to be transformed into a clean **Silver Delta Table**.

---

## 🎯 Task

Perform the following transformations:

1. **Read** the input data from `listings.json` using Spark.  
2. **Explode** the `amenities` array so that each row contains a single amenity.  
3. **Normalize** boolean-like columns (e.g., `"true"`, `"false"`, `"yes"`, `"no"`) into proper boolean (`True` / `False`) Spark data types.  
4. **Rename** or select only the relevant columns for downstream use.  
5. **Save** the cleaned DataFrame in **Delta format** to the **Silver layer** path:  
   `/Volumes/practice_db_catalog/airbnb/data_volume/airbnb/silver/listings.json` 

---

## 🧩 Assumptions

- The input file `listings.json` exists in the **Bronze** path:  
  `/Volumes/practice_db_catalog/airbnb/data_volume/airbnb/raw/listings.json`
- The `amenities` field may contain an array or a stringified array.  
- Boolean columns may contain values like `"TRUE"`, `"Yes"`, `"0"`, `"1"`, etc.  
- The final cleaned DataFrame should contain only essential columns:  
  `id`, `name`, `amenity`, `has_parking`, and `is_superhost`.  
- Handle missing or malformed columns gracefully (e.g., cast to `null`).  

---

## 📦 Deliverables

- **Output Format:** Delta table written to Silver  
- **Output Path:** `/Volumes/practice_db_catalog/airbnb/data_volume/airbnb/silver/listings.json`

| **Expected Columns** | `id`, `name`, `amenity`, `has_parking`, `is_superhost` |

---

## 🧠 Notes

- Use `pyspark.sql.functions.explode()` to expand the amenities array.  
- Use `F.when()` or `F.col().cast("boolean")` for boolean normalization.  
- Use clear column aliases for readability.  
- Validate the write by reading from the Silver path and displaying the first few rows.

---

## 🧩 Example Output (simplified)

| id  | name               | amenity          | has_parking | is_superhost |
|-----|--------------------|------------------|--------------|---------------|
| 101 | Cozy Beach House   | Wifi             | true         | false         |
| 101 | Cozy Beach House   | Ocean View       | true         | false         |
| 102 | City Apartment     | Air Conditioning | false        | true          |

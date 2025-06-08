# 🏬 Olist Data Warehouse

This repository contains a dimensional data warehouse design for **Olist**, along with its **Slowly Changing Dimension (SCD)** strategy. It also includes a modular **ELT pipeline** using Python and **Luigi** for orchestration.

---

## 📌 Project Overview

Olist is a major e-commerce marketplace in Brazil, connecting small businesses with customers across the country. As the business grows, so does the complexity of handling its transactional data. This repository showcases:

- A dimensional model designed to support Olist's analytics needs
- A clear SCD strategy to handle changes in dimension tables
- A working ELT pipeline built with Python and Luigi

---

## 📋 Requirements Gathering – SCD Strategy (Type 1)

As part of the design process, we conducted a requirements gathering session (simulated) with stakeholders to understand how they want to handle changes in dimension attributes.

Below are key questions and responses:

### Q1: Should we track historical changes to product categories or attributes?
**Answer:** "No, we only need current product details. Overwrite old values if changes occur."

### Q2: Do seller details change and need history?
**Answer:** "Seller locations rarely change. Just update current values."

### Q3: Should we preserve old customer addresses?
**Answer:** "No, only the latest location matters for logistics."

### Q4: What about order status changes?
**Answer:** "Track status changes in fact tables only."

---

## 🧠 SCD Strategy Summary

Based on stakeholder feedback, we chose a **Type 1 SCD** strategy — overwrite old values without keeping historical versions.

| Dimension         | SCD Type | Change Handling Description            |
|------------------|----------|----------------------------------------|
| `dim_product`     | Type 1   | Overwrite category and attributes      |
| `dim_seller`      | Type 1   | Overwrite location information         |
| `dim_customer`    | Type 1   | Overwrite address fields               |
| `dim_order_status`| Type 1   | Overwrite status description           |

### ✅ Why Type 1?

- **Aligned with business needs** – Stakeholders do not require historical tracking
- **Simpler maintenance** – No versioning or date range logic
- **More efficient** – Reduces storage use and ETL complexity
- **Straightforward updates** – Clean `UPDATE` statements instead of inserts

---

## ⚙️ ELT Pipeline – Python & Luigi

This project uses **Luigi** to orchestrate a modular ETL process implemented in Python and SQL.

### 📂 Pipeline Structure

| Script         | Purpose                                |
|----------------|----------------------------------------|
| `extract.py`   | Connects to source DB and extracts raw data |
| `load.py`      | Loads raw data into staging/DWH tables |
| `transform.py` | Applies transformations and loads final tables |

### ▶️ How to Run

1. Clone the repository  
2. Create a `.env` file with database connection info:
   ```env
   SRC_POSTGRES_DB=olist_src
   SRC_POSTGRES_HOST=localhost
   SRC_POSTGRES_USER=your_username
   SRC_POSTGRES_PASSWORD=your_password
   SRC_POSTGRES_PORT=5433

   DWH_POSTGRES_DB=olist_dwh
   DWH_POSTGRES_HOST=localhost
   DWH_POSTGRES_USER=your_username
   DWH_POSTGRES_PASSWORD=your_password
   DWH_POSTGRES_PORT=5434
3. Start PostgreSQL services:
    ```
    docker-compose up -d
    ```
4. Set up your Python environment:
    ```
    python -m venv venv
    source venv/bin/activate   # On Windows use venv\Scripts\activate
    pip install -r requirements.txt

    ```
5. Run Luigi tasks:
    ```
    luigi --module elt_main --local-scheduler
    ```

## 📎 Notes
This setup is for learning and prototyping. For production use, additional error handling and monitoring would be needed.

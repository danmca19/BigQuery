# 🧠 Business Insight Automation with BigQuery and Python

This project demonstrates how to solve a real-world business problem using **data filtering, transformation, and automation** via **BigQuery** and **Python**. The main objective is to isolate and analyze **closed complaints**, enabling operational teams to focus on process efficiency and service resolution KPIs.

---

## 📌 Business Problem

In many customer service or regulatory environments, understanding the volume and nature of **closed complaints** is essential for:

- Tracking service resolution rates
- Identifying complaint categories with faster closures
- Measuring operational team performance
- Supporting compliance reporting

However, this data often lives in large, raw datasets within cloud storage solutions like BigQuery, making **manual analysis slow and error-prone**.

---

## 🎯 Solution Overview

This project solves the problem by:

- **Reading raw data** from a BigQuery complaints table
- **Filtering records** with status `"closed"`
- **Writing the result** back into a clean, dedicated table in BigQuery
- Allowing the data to be used downstream (dashboards, reports, machine learning)

By automating the filtering logic and using parameterized functions, the solution can be **reused across teams and domains** with minimal changes.

---

## ⚙️ Tools and Technologies

- **Google BigQuery** – cloud data warehouse for structured data
- **Python (pandas + google-cloud-bigquery)** – data processing and cloud access
- **GCP IAM** – for secure access and permission control


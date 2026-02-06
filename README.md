# 🚀 Data Pipeline with Apache Airflow

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE)
![ETL](https://img.shields.io/badge/ETL-Data%20Engineering-green)
![Status](https://img.shields.io/badge/Status-Study%20Project-yellow)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

## 📌 Project Overview

This project is a **data pipeline built with Apache Airflow**, designed to orchestrate an **ETL process** (Extract, Transform, Load).

The main goal is to practice **data engineering fundamentals**, including task orchestration, scheduling, modular pipelines, and environment setup.

---

## 🏗️ Architecture

The pipeline follows a classic ETL structure:

1. **Extract**  
   - Reads raw data from source files (e.g. CSV / JSON).

2. **Transform**  
   - Applies basic data cleaning and transformations using Python.

3. **Load**  
   - Saves the processed data to a structured output (e.g. Parquet or processed CSV).

Airflow manages:
- Task dependencies  
- Scheduling  
- Retries and monitoring  

---

## ⚙️ Tech Stack

- **Python 3.11**
- **Apache Airflow**
- **Pandas**
- **Virtual Environment (.venv)**
- **Linux-based development**

---

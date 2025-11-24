# Toronto Crime Data ETL & Visualization Platform

A full-stack project that processes, analyzes, and visualizes Toronto crime data from **January 2014 to September 2024**. It combines big data processing with interactive dashboards and a dynamic frontend to deliver clear insights into crime patterns across the city.

---

## Project Overview

This platform includes an end-to-end ETL pipeline using **PySpark**, data storage in **MySQL**, visualization through **Tableau**, and an interactive **React** frontend.

---

## Key Features

### ETL Pipeline (PySpark)
- Extracted public Toronto crime datasets.
- Transformed and filtered data to retain only relevant records, removing noise for optimized performance.
- Loaded cleaned data into a **MySQL** database.
- **Docker**, **Kubernetes**, and **Terraform** to deploy ETL Workflow

### Visual Analytics (Tableau)
- Designed interactive dashboards for temporal and spatial crime trends.
- Built a **Toronto crime map** visualizing incidents by type and frequency.
- Includes filtering by crime type, time period, and geographic region.

### Frontend Interface (React)
- Developed a responsive **React web app** for users to explore data dynamically.
- Search, filter, and visualize incidents with an intuitive UI.
- Fetches real-time crime data from the backend database.

---

## Tech Stack

| Component   | Tools / Technologies        |
|-------------|------------------------------|
| ETL         | PySpark, Python              |
| Database    | MySQL                        |
| Visualization | Tableau                    |
| Frontend    | React, JavaScript, HTML/CSS  |

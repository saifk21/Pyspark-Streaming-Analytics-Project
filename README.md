# 🚀 Real-Time Streaming Analytics with PySpark

This project demonstrates a complete **real-time data engineering pipeline** using **PySpark Structured Streaming**.

Instead of batch processing static datasets, this project simulates **live streaming data** and performs real-time analytics such as:

- Live sales dashboard
- Top-N product analysis
- Session-based user analytics
- Late data handling with watermarking

---

## 🧠 Key Concepts Covered

### ⏱️ Windowing
- Tumbling Windows
- Sliding Windows
- Session Windows

### ⚡ Streaming Fundamentals
- Event-time processing
- Micro-batch execution
- Watermarking (late data handling)

### 📊 Analytics
- Top-N products per region
- Session-level revenue analysis
- Real-time aggregations

---

## 🏗️ Architecture

![Architecture](assets/architecture.svg)

---

## 🖥️ Sample Output

![Console Output](assets/console_sliding.svg)

---

## 📂 Project Structure


pyspark-streaming-analytics/
│
├── src/
│ ├── 01_sliding_window_topn.py
│ ├── 02_late_event_simulation.py
│ ├── 03_realtime_topn.py
│ ├── 04_session_windows.py
│ ├── 05_session_full_pipeline.py
│ ├── 06_sales_dashboard.py
│ └── 07_session_theory.py
│
├── assets/
│ ├── architecture.svg
│ ├── console_sliding.svg
│
├── requirements.txt
└── README.md




---

## 🛠️ Tech Stack

- PySpark (Structured Streaming)
- Python
- Faker

---

## 🚀 How to Run

### 1. Install dependencies

```bash
pip install -r requirements.txt


📈 Use Cases
Real-time dashboards
Fraud detection
User behavior analytics
Streaming KPI monitoring
🔮 Future Improvements
Kafka integration
Delta Lake storage
Streamlit dashboard
👨‍💻 Author

Built as part of hands-on learning in real-time data engineering

⭐ If you like this project, give it a star!



#  Eco-Pulse Lakehouse

## Real-Time Wildfire Risk Monitoring System

**Author:** Raúl Jiménez Delgado
**Contact:** [raul.jimenez.del@gmail.com](mailto:raul.jimenez.del@gmail.com)
**LinkedIn:** [linkedin.com/in/rauljimenezdelgado](https://linkedin.com/in/rauljimenezdelgado)

---

##  Project Overview

**Eco-Pulse Lakehouse** is an end-to-end **distributed data engineering system** designed to monitor **wildfire risk in real time**. The platform ingests **satellite fire telemetry** and **IoT-based weather sensor data** to assess evacuation and operational risks for **critical infrastructure**, such as tourism operations in the **Canary Islands**.

The solution is built on a **Lakehouse architecture with Lambda-style processing**, ensuring:

* ACID-compliant data management
* Horizontal scalability
* Reliable historical data retention

A core component of the system is the automated correlation of **active fire fronts** with **meteorological conditions**, applying the **30-30-30 Rule**:

> *  Wind speed > **30 km/h**
> *  Temperature > **30 °C**
> *  Relative humidity < **30%**

These parameters are used to classify wildfire risk levels dynamically.

---

##  Business Case

Decision-making in high-risk wildfire zones requires **low-latency, real-time intelligence**. Traditional BI and reporting systems often fail to correlate **heterogeneous data sources**—such as satellite imagery and localized weather stations—quickly enough to support emergency response.

###  Objective

Reduce emergency response time by providing a **live geospatial dashboard** that:

* Visualizes active fire events
* Calculates fire spread potential
* Incorporates wind direction and intensity in real time

---

##  System Architecture

The project follows the **Medallion Architecture** (Bronze, Silver, Gold) within a **Data Lakehouse** paradigm.

### Data Layer Definitions

####  Bronze Layer — *Raw Data*

* Ingests raw JSON payloads from:

  * **NASA FIRMS** (VIIRS / MODIS satellites)
  * **OpenWeatherMap API**
* Data is streamed via **Apache Kafka**
* Stored in raw format for:

  * Auditability
  * Replayability

---

####  Silver Layer — *Cleansed & Enriched*

* Processed using **Apache Spark Structured Streaming**
* Key transformations:

  * Deduplication
  * Schema enforcement
  * Type casting (coordinates → Float, timestamps → Datetime)
  * Time and spatial partitioning
* Persisted using **Delta Lake** for transactional guarantees

---

####  Gold Layer — *Business Aggregations*

* Batch-processed analytical tables
* Performs:

  * Geospatial joins between fire events and weather stations
  * Risk classification using the **30-30-30 logic**
* Optimized for downstream analytics and visualization

---

##  Technology Stack

###  Ingestion

* Python
* Apache Kafka
* Zookeeper

###  Processing

* Apache Spark (PySpark)
* Structured Streaming

### Storage

* Delta Lake
* MinIO (S3-compatible object storage)

###  Infrastructure & Orchestration

* Docker
* Docker Compose

###  Visualization

* Streamlit
* PyDeck (3D geospatial rendering)
* Altair

###  Language

* Python **3.12**

---

##  Project Structure

```text
eco-pulse-lakehouse/
│
├── config/                 # Spark and system configuration
│   └── spark_config.conf
│
├── data/                   # Local Lakehouse storage (gitignored)
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── docker/                 # Custom Docker images
│
├── notebooks/              # EDA and API exploration
│   └── 01_explore_nasa_api.ipynb
│
├── src/                    # Application source code
│   ├── ingestion/          # Kafka producers
│   │   ├── nasa_producer.py
│   │   └── weather_producer.py
│   ├── processing/         # Spark streaming and batch jobs
│   │   ├── fire_risk_processor.py
│   │   └── gold_batch_job.py
│   ├── utils/              # Spark helpers and utilities
│   │   └── spark_utils.py
│   └── visualization/      # Streamlit dashboard
│       └── app.py
│
├── .env                    # Environment variables (API keys)
├── .gitignore
├── docker-compose.yml      # Infrastructure definition
├── requirements.txt        # Python dependencies
└── README.md
```

---

##  Data Pipeline Flow

### 1️ Data Ingestion

* Python producers query:

  * NASA FIRMS (fire events)
  * OpenWeatherMap (weather telemetry)
* Events are serialized and published to Kafka topics:

  * `fire-events`
  * `weather-events`

---

### 2️ Stream Processing (Silver Layer)

* Spark Structured Streaming consumes Kafka topics
* Performs:

  * Data cleansing
  * Enrichment
  * Normalization
* Writes transactional Delta tables
* Fault tolerance via **checkpointing**

---

### 3️ Risk Engine (Gold Layer)

* Batch Spark job computes:

  * Distance between fire coordinates and weather stations
  * Wind vector influence
* Applies the **30-30-30 rule** to assign risk levels:

  * Moderate
  * High
  * Very High
  * Extreme

---

### 4️ Visualization

* Streamlit app queries Gold Delta tables
* Uses native Delta reader for high performance
* Renders a **3D geospatial wildfire risk dashboard**

---

##  Setup & Installation

### Prerequisites

* Docker Desktop **4.0+**
* Python **3.10+**
* Git

---

### 1️ Clone the Repository

```bash
git clone https://github.com/rauljimenezdelgado/eco-pulse-lakehouse.git
cd eco-pulse-lakehouse
```

---

### 2️ Environment Configuration

Create a `.env` file in the project root:

```bash
NASA_MAP_KEY=your_nasa_firms_key
OPENWEATHER_API_KEY=your_openweather_key
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

---

### 3️ Deploy Infrastructure

Start Kafka, Spark, and MinIO services:

```bash
docker-compose up -d
```

---

### 4️ Run the Data Pipeline

#### Step A — Start Producers

```bash
# Terminal 1
python src/ingestion/nasa_producer.py

# Terminal 2
python src/ingestion/weather_producer.py
```

---

#### Step B — Start Streaming Processor (Silver Layer)

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  src/processing/fire_risk_processor.py
```

---

#### Step C — Run Risk Engine (Gold Layer)

```bash
spark-submit \
  --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  src/processing/gold_batch_job.py
```

---

### 5️ Launch Dashboard

```bash
streamlit run src/visualization/app.py
```

Access the UI at: **[http://localhost:8501](http://localhost:8501)**

---

##  Key Engineering Concepts

* **Decoupling:** Kafka absorbs ingestion bursts from satellite feeds
* **ACID Transactions:** Delta Lake guarantees consistency under concurrent workloads
* **Geospatial Analytics:** Distributed Haversine / Euclidean distance calculations
* **Fault Tolerance:** Spark checkpointing enables exactly-once recovery
* **Time Travel:** Delta Lake allows historical queries for auditing and replay

---

##  Future Enhancements

* Machine Learning–based fire spread prediction
* Alerting system (SMS / email / webhook)
* Integration with civil protection APIs
* Multi-region cloud deployment

---

> **Eco-Pulse Lakehouse** demonstrates production-grade data engineering principles applied to real-world environmental risk monitoring.

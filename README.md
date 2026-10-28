# Real-Time Financial Telemetry Platform (2026)

## 1. Overview
This platform implements a **Kappa Architecture** to ingest, aggregate, and visualize high-velocity Bitcoin (BTC) trade data from the Binance Exchange. It utilizes **Event-Time semantics** to ensure financial accuracy (VWAP) despite potential network jitter.

## 2. Technology Stack
* **OS:** Ubuntu 24.04 LTS (Noble Numbat)
* **Ingestion:** Apache Kafka 4.0 (KRaft Mode)
* **Processing:** Apache Spark 4.0 (Structured Streaming)
* **Visualization:** Streamlit (Fragment Architecture)
* **Language:** Python 3.12+

## 3. Infrastructure Configuration (VirtualBox 7.1)
To prevent I/O bottlenecks and "Black Screen" issues on Ubuntu 24.04, the following specifications are mandatory:

1.  **Hardware Resources:** Allocate a minimum of **8GB RAM** and **4 vCPUs** to support the JVM stack.

2.  **Display Settings:** Set Graphics Controller to **VMSVGA**, increase Video Memory to **128MB**, and strictly **disable 3D Acceleration** to avoid Wayland conflicts.

3.  **Storage Strategy:** Use a **50GB Fixed Size (Pre-allocated)** disk to eliminate hypervisor write jitter during Kafka ingestion.

4.  **CPU Optimization:** Set the Linux CPU governor to `performance` to minimize interrupt latency.

## 4. Deployment Instructions

### Phase A: Infrastructure Initialization (Docker)
Navigate to the docker directory and launch the Kafka 4.0 KRaft broker:

```bash
cd docker
docker compose up -d

# Create Raw Trade Topic
docker exec -it kafka kafka-topics.sh --create --topic raw_crypto_trades --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Create Aggregated Stats Topic
docker exec -it kafka kafka-topics.sh --create --topic aggregated_crypto_stats --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

Phase B: Pipeline Execution

Run the following commands in separate terminal tabs to activate the full pipeline:

    Ingestion Layer

        Command: python src/producers/api_producer.py

        Function: Polls Binance API v3 (Trades) while managing rate limits (6,000 weight/min).

    Processing Layer

        Command: python src/processing/streaming_job.py

        Function: Executes Spark 4.0 Structured Streaming with 1-minute tumbling windows.

    Visualization Layer

        Command: streamlit run src/dashboard/app.py

        Function: Launches the reactive dashboard at http://localhost:8501.
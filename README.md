# Real-Time Financial Telemetry Platform (2026)

## 1. Overview
[cite_start]This platform implements a **Kappa Architecture** to ingest, aggregate, and visualize high-velocity Bitcoin (BTC) trade data from the Binance Exchange[cite: 9, 10]. [cite_start]It utilizes **Event-Time semantics** to ensure financial accuracy (VWAP) despite potential network jitter[cite: 17, 18].

## 2. Technology Stack
* [cite_start]**OS:** Ubuntu 24.04 LTS (Noble Numbat) [cite: 13]
* [cite_start]**Ingestion:** Apache Kafka 4.0 (KRaft Mode) [cite: 12, 175]
* [cite_start]**Processing:** Apache Spark 4.0 (Structured Streaming) [cite: 12, 331]
* [cite_start]**Visualization:** Streamlit (Fragment Architecture) [cite: 12, 428]
* [cite_start]**Language:** Python 3.12+ [cite: 14]

## 3. Infrastructure Configuration (VirtualBox 7.1)
To prevent I/O bottlenecks and "Black Screen" issues on Ubuntu 24.04, the following specifications are mandatory:

1.  [cite_start]**Hardware Resources:** Allocate a minimum of **8GB RAM** and **4 vCPUs** to support the JVM stack[cite: 47, 50].

2.  [cite_start]**Display Settings:** Set Graphics Controller to **VMSVGA**, increase Video Memory to **128MB**, and strictly **disable 3D Acceleration** to avoid Wayland conflicts[cite: 65, 66, 67].

3.  [cite_start]**Storage Strategy:** Use a **50GB Fixed Size (Pre-allocated)** disk to eliminate hypervisor write jitter during Kafka ingestion[cite: 55, 57].

4.  [cite_start]**CPU Optimization:** Set the Linux CPU governor to `performance` to minimize interrupt latency[cite: 113].

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
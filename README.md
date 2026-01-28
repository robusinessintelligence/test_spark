# Ingestions of Reports:
    - Customers
    - Orders
    - Events

---

### Architecture & Design

#### Overall pipeline architecture
- The architecture is divided into two stages, raw_data and curated_data
raw_data it's data from folder input (landing) here i make some transformations 
to have sure the quality of data

- On curated_data i take care of duplicate data if some reprocess happens 
and make some aggregations to build final reports.

#### Data flow from ingestion to output

---

### Technical Decisions

#### Why this language and approach?

#### Batch vs streaming considerations

#### How idempotency and reprocessing would work

---

### Production Readiness

#### How this would run in:
- Snowflake
- AWS / Azure / GCP

#### Where dbt, Airflow, or similar tools would fit

---

### Data Governance

#### Handling PII

#### Data quality checks

#### Schema evolution strategy

---

### Scaling
- What changes if data volume grows 100x?
- Where are the current bottlenecks?

---

### Delivery Instructions
Please submit:
- Source code (public Git repository)
- README.md
- Clear instructions on how to run the project locally


<!-- stop docker -->
docker compose -f dataproc/spark-docker-compose.yml down

<!-- to run docker -->
docker compose -f dataproc/spark-docker-compose.yml up -d --scale spark-worker=3 

<!-- run job -->
docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/raw_data/customers.py
docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/raw_data/customers.py '{"_PROCESS_DATE": "2026-01-01"}'
docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/curated_data/dim_customer.py

docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/raw_data/orders.py
docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/curated_data/fact_orders.py

docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/raw_data/events.py [streaming]
docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/curated_data/fact_events_streaming.py [streaming]

docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/curated_data/fact_events.py
docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/curated_data/fact_events_check_data.py
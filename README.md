# Ingestions of Reports:
    - Customers
    - Orders
    - Events

---

### Architecture & Design

#### Overall pipeline architecture
- The architecture is divided into two stages, raw_data and curated_data.
raw_data it's data from folder input (landing) here i make some transformations 
to have sure the quality of data

- On curated_data i take care of duplicate data if some reprocess happens 
and make some aggregations to build final reports.

#### Data flow from ingestion to output

- Jobs in process/raw_data already for commom loads or reprocessing.

commom load:
if it's the first time, a new partition will be created into the folder of that topic

reprocesssing:
if not, jobs in raw_data are ready to receive parameters like _PROCESS_DATE,
for example when we pass a parameter like "_PROCESS_DATE": "2026-01-1"
the data from folder input/customers/2026-01-01 will be reprocessed, and the data in this folder
will be overwrited at the same folder that exists in structure of raw_data.


- Jobs in process/curated_data:
Read all data into the folder raw_data of the same topic, if it's necessary to make some filter, the structure of folder raw_data it's already ready, because field processing_date exists for all jobs

- Streaming processes:
Streaming processes also have processing_date if it's necessary to make some analysis in the future but doesn't execept the param _PROCESS_DATE they run in microbatches at real time.

---

### Technical Decisions

#### Why this language and approach?
- The language python it's very popular, and has a low learning curve, so it's easier to find people who work with that

- About the approach, i had little time, it's a test and i have to handle with my tasks in my current job.
In a perfect world or future job, i would make this pipeline with Medallion Architecture, or other taking care about the costs with
one layer for landing and bronze, silver and gold layers of raw and curated or data mesh, depends which costs we have to take care 
when we talk about storage billions of data and business rules complexity.

#### Batch vs streaming considerations
- Batch has less costs with processing, and the hour whe the data it's avaiable for consume need to be aling with the client
- Streaming has more costs with processing, and have to take care with errors and data at the pipeline to validate, reject and analyse in that case 
we always have to be ahead.

#### How idempotency and reprocessing would work
- the both idempotency and reprocessing, works in the same way, if the pipeline reprocess the same data 100 times, it will be the same data. 
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
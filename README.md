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

- About the approach, i had little time, i have to handle with my tasks in my current job.
In a perfect world, i would make this pipeline with Medallion Architecture, or other taking care about the costs with
one layer for landing, bronze, silver and gold layers or raw_data and curated_data or data mesh, depends which costs we have to take care 
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
- AWS / Azure / GCP

- GCP
    - Documentation of Architecture with details about the process and one Draw with architecture.

    - Trigger to start the process (Scheduler or Cloud Function to check data and Run the Job)

    - Cloud Tasks if it's necessary to reschedule the execution

    - Job 1 in Dataproc to process data and storage at bucket
    - Integration tests in pipeline
    - check data quality on dataproc or other way

    - Job 2 in Dataproc to process data and storage at bucket or Bigquery
    - Integration tests in pipeline
    - check data quality on dataproc or other way

    - ingestion at BigQuery partitioning table for performance

    - End with some visualization tool as PowerBI or Tableu or etc.

- Azure
    - Documentation of Architecture with details about the process and one Draw with architecture.

    - Trigger to start the process (Azure Data Factory, or some Azure web function)

    - Job 1 in Databricks to process data and storage at bucket
    - Integration tests in pipeline
    - check data quality on databricks or other way

    - Job 2 in Databricks to process data and storage at bucket or Bigquery
    - Integration tests in pipeline
    - check data quality on databricks or other way

    - ingestion at Unity Catalog partitioning table for performance

    - End with some visualization tool as PowerBI or Tableu or etc.

- Snowflake or AWS
    I dont have experience work with snow flake or AWS but the concepts are the same of pipelines above

    - Documentation of Architecture with details about the process and one Draw with architecture.

    - Trigger to start the process

    - Job 1 in Spark to process data and storage at some bucket (datalake)
    - Integration tests in pipeline
    - check data quality on databricks or other way

    - Job 2 in Spark to process data and storage at some bucket (datalake)
    - Integration tests in pipeline
    - check data quality on databricks or other way

    - ingestion at some Datawarehouse partitioning table for performance

    - End with some visualization tool as PowerBI or Tableu or etc.

#### Where dbt, Airflow, or similar tools would fit
    - DBT could make some business rules validation, it can be made at Spark, and could make data lineage

    - Airflow could orchestrate de process and start the process with some schedule trigger or some sensor
    but airflow it's always runing, even if any process are running.


---

### Data Governance
- It's could be made with polices at Bigquery and access management at the datasets and at the column of tables (Column-level security).
- It's could be made with polices at Unity Catalog and access management at the catalogs and at the column of tables (Column-level security).

Both of them could me made with retrict access of visualization tool, like a new layer of access control.

#### Handling PII
Could be made with polices applied to columns to data masking (Hashing / Pseudonymization)

#### Data quality checks
Could be made in middle step before delivey data do consuming of client. (Using DBT, Spark or any other tool with this function)

#### Schema evolution strategy
It's not the only solution, but i like to block the schema at bronze or raw layer and work with merge schema at silver or curated layer.
This blocks incorrect fields during loading. if there is some new field at the schema, i can add it to the model of schema in pyspark or other tool.

---

### Scaling
- What changes if data volume grows 100x?
I will need more workers

- Where are the current bottlenecks?
This is a load for especific table and registers, if its go to production, we need to add some resources like datawarehouse,
more workers, terraform for infrastructure, a trigger for the process, quality checks, integrations tests.

---

### Delivery Instructions
Please submit:
- Source code (public Git repository)
- README.md
- Clear instructions on how to run the project locally

---
### Install Instructions

1. First Step
[Install docker](https://docs.docker.com/get-started/get-docker/)

2. Git clone
[Git Clone](https://github.com/robusinessintelligence/test_spark.git)

Example command:

```bash
git clone https://github.com/robusinessintelligence/test_spark.git
```

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
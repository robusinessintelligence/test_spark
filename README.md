# test_spark

<!-- stop docker -->
docker compose -f dataproc/spark-docker-compose.yml down

<!-- to run docker -->
docker compose -f dataproc/spark-docker-compose.yml up -d --scale spark-worker=3 

<!-- run job -->
docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/raw_data/customers.py
docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/raw_data/customers.py '{"_PROCESS_DATE": "2026-01-01"}'
docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/curated_data/dim_customer.py

docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/raw_data/orders.py

docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/raw_data/events.py [streaming]
docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/curated_data/fact_events_streaming.py [streaming]

docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/curated_data/fact_events.py
docker exec -it spark-master /opt/spark/bin/spark-submit /jobs/process/curated_data/fact_events_check_data.py
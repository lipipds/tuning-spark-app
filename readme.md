# Apache Spark & Trip Record Data Analysis

### Green Taxi Trip Record Data: ?

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### verify local spark
```shell
pip install -r requirements.txt
pyspark --version

spark-submit --help
http://localhost:4040/jobs/
```

### build spark docker images [spark & history server]
```shell
docker build -t owshq-spark:3.5 -f Dockerfile.spark . 
docker build -t owshq-spark-history-server:3.5 -f Dockerfile.history .
```

### create .env file for variables
```shell
.env 
```

### run spark cluster & history server on docker
```shell
docker-compose up -d
docker ps

docker logs spark-master
docker logs spark-worker-1
docker logs spark-worker-2
docker logs spark-history-server
```

### run spark job on spark cluster
```shell
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/etl-rides-fhvhv.py
```


### destroy spark cluster & history server
```shell
docker-compose down
```
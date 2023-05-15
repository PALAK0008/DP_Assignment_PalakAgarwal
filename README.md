This project contains data pipeline. Schiphol flight dataset is used for implementation. Pipeline is doing the ETL process on the data.
    - Data is extracted from REST API and stored in csv.
    - Csvs are read in the dataframes which are transformed according to scenario.
    - Finally transformed data is stored in postgres tables.

## Flow Diagram -

![alt text](https://github.com/PALAK0008/DP_Assignment_PalakAgarwal/blob/main/images/flow_diagram.jpg?raw=true)

## Scenarios covered -

Scenario 1 - show the flights which are arrived by the delay of 10 seconds from their actual landing time

Scenario 2 - show the dataset to get the number of flights fly to specific country per day

## Pre-requisite-

* Docker should be installed on your machine

## Things to do;

*  Clone the Github repository 
*  Build the Spark and Airflow images
*  Start and run the Spark and Airflow containers 
*  Copy postgres jar and python files in the spark containers
*  Run the Spark jobs and confirm data in postgres database.

## Clone the Github repository.
```bash
git clone https://github.com/PALAK0008/DP_Assignment_PalakAgarwal.git
```

## Build the Spark image.
```bash
sudo docker build -f ./docker/Dockerfile.Spark . -t spark-air
```

## Build the Airflow image.
```bash
sudo docker build -f ./docker/Dockerfile.Airflow . -t airflow-spark
```
### Provide access to all folders.
``` bash
sudo chmod -R 777 *
```
## Start and run the Spark and Airflow containers.
```bash
sudo docker-compose -f ./docker/docker-compose.Spark.yaml -f ./docker/docker-compose.Airflow.yaml up -d
```
When all the services all started successfully, then go to -
http://localhost:8080/ to check that Airflow
http://localhost:8090/ that Spark is up and running. 
http://localhost:15432/ to check Postgres GUI


* Run the Spark jobs with below command - 

```bash
sudo docker exec -it <Spark-Worker-Contianer-name> \
    spark-submit --master spark://XXXXXXXXXXXXXX:7077 \
    spark_etl_script_docker.py
```

* if successful, the schedule the spark jobs in Airflow
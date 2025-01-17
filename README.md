# SF Crime Data


## Create kafka topic 

```
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sf-data
```

## To run consumer

```
python consumer_server.py
```

## Questions
`1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?`

Ans: Changing the SparkSession property made the processing faster and it could pprocess more data.

`2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?`

Ans: Some efficient SparkSession property : 
```
spark.executor.memory  = 1g
spark.driver.memory = 4g
spark.default.parallelism = 8
spark.sql.shuffle.partitions = 8
```
Note: I have 4 core 16gb machine.
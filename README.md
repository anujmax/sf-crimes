# SF Crime Data



## Create kafka topic 

```
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sf-data
```

## To run consumer

```
python consumer_server.py
```
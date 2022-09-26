# CS-523 Big Data Project -
## Tweet Stream analyzer

- Anju Majoka # 612719


![Apache|Hadoop](https://upload.wikimedia.org/wikipedia/commons/thumb/0/0e/Hadoop_logo.svg/1280px-Hadoop_logo.svg.png)

## Project Technologies
- Twitter SDK
- Hadoop
- Kafka
- Spark
- Hive
- HBase
- Java 8

## Features

- Consume Tweets from Tweeter using Tweeter streaming api & Tweeter SDK.
- Converts Tweets recevied  from streaming api to CSV lines.
- Publish/Stream each csv line/tweets to Kafka topic
- Consume the streaming data from kafka topic, using Spark streaming API.
- Use Spark Streaming API to stream the data directly to Hbase & HDFS

## Architectural Diagram

![Architectural Diagram](https://drive.google.com/uc?export=view&id=1XwbntiF-0FOueAg1mOzkUWnJQ9NgRjC9)

## Enviroment Setup
Install following software
- [Hadoop](https://medium.com/beeranddiapers/installing-hadoop-on-mac-a9a3649dbc4d) 
- [Kafka](https://kafka.apache.org/) 
- [Maven](https://maven.apache.org/download.cgi)

Install the dependencies

```sh
mvn clean install
```

Start zookeeper ...

```sh
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

```
Start Kafka and create topic

```sh
kafka-server-start /usr/local/etc/kafka/server.properties
kafka-topics --create --topic TwitterDataAnalytics --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2
```

Start Hadoop

```sh
cd /usr/local/Cellar/hadoop/3.3.4/sbin
./start-all.sh
```

Verify all process are up and running

```sh
JPS
```

Start HBASE and create table

```sh
cd /usr/local/Cellar/hbase/2.4.10/libexec/bin
./start-hbase.sh
hbase shell
create 'CS523_Tweets', 'tweet_data'
```

## Programs to run

| Program | Purpose |
| ------ | ------ |
| [StreamTweet.java](BigDataProject-CS523/src/main/java/cs523/finalproject/tweet/) | Reads data from tweeter streaming api and publishes to kafka
| [StreamingDataToHbase.java](BigDataProject-CS523/src/main/java/cs523/finalproject/spark/hbase/)  | Consume streaming data from kafka topic and store it to Hbase table  |
| [StreamDataToHive.java](BigDataProject-CS523/src/main/java/cs523/finalproject/spark/hive/)  | Consume streaming data from kafka topic and store it to HDFS

# spark-structured-streaming

# Squadron Setup for Spark development

## initial setup
´´
curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
wget http://downloads.lightbend.com/scala/2.13.0/scala-2.13.0.rpm
yum install -y sbt git maven scala-2.13.0.rpm 
``

## Clone repo
git clone https://github.com/kartik-dev/spark-structured-streaming.git
cd spark-structured-streaming

## Build and deploy
sbt clean package
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --class org.codait.streaming.SparkJobSimple target/scala-2.11/integration-pattern-mqtt-spark_2.11-0.1-SNAPSHOT.jar

## Kafka topic creation
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create \
    --zookeeper localhost:2181 \
    --replication-factor 1 \
    --partitions 1 \
    --topic test

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper localhost:2181


git clone https://github.com/kartik-dev/spark-structured-streaming.git

git remote add upstream https://github.com/kartik-dev/spark-structured-streaming.git




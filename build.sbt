
name := "integration-pattern-mqtt-spark"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("org.apache.activemq" % "activemq-all" % "5.15.3",
  "org.apache.activemq" % "activemq-mqtt" % "5.15.3",
  "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.0",
  "org.apache.kafka" % "kafka-clients" % "0.10.2.0",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.3.0")

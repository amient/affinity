package io.amient.affinity.spark

import java.util.Properties

import io.amient.affinity.kafka.{KafkaClient, KafkaClientImpl}

import scala.collection.JavaConverters._

object TMP extends App {

  val impl = new KafkaClientImpl("graph", new Properties() {
    put("bootstrap.servers", "localhost:9092,localhost:9091")

  })

  impl.topicOffsets(KafkaClient.EARLIEST_TIME).asScala.foreach(println)
  impl.topicOffsets(KafkaClient.LATEST_TIME).asScala.foreach(println)
}

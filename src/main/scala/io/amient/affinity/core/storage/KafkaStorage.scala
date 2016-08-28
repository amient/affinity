package io.amient.affinity.core.storage

import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

import scala.collection.JavaConverters._

abstract class KafkaStorage[K,V](topic: String, partition :Int) extends Storage[K, V] {

  def boot(isMaster: () => Boolean): Unit = {

    //TODO configure KafkaStorage via appConfig and replace prinln(s) with log.info
    println(s"Bootstrapping memstore partition from kafka topic: `$topic`, partition: $partition")
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", "localhost:9092")
    consumerProps.put("enable.auto.commit", "false")
    consumerProps.put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    consumerProps.put("value.deserializer", classOf[ByteArrayDeserializer].getName)

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
    try {
      val tp = new TopicPartition(topic, partition)
      val consumerPartitions = util.Arrays.asList(tp)
      consumer.assign(consumerPartitions)
      consumer.seekToEnd(consumerPartitions)
      val records = consumer.poll(1000L)
      val lastOffset = consumer.position(tp)
      println(s"Latest offset in kafka topic: `$topic`, partition: $partition, offset: $lastOffset")

      consumer.seekToBeginning(consumerPartitions)
      val continue = new AtomicBoolean(true)
      while (continue.get) {
        val records = consumer.poll(1000)
        for (r <- records.iterator().asScala) {
          val (key, value) = deserialize(r.key(), r.value())
          update(key, value)
        }
        if (consumer.position(tp) >= lastOffset) {
          if (isMaster()) continue.set(false)
        }
      }
    } finally {
      consumer.close
      //TODO after becoming a master there can only be termination because we're closing the consumer
    }

  }
  println(s"Finished bootstrap, kafka topic: `$topic`, partition: $partition; preparing producer...")
  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", "localhost:9092")
  producerProps.put("acks", "all")
  producerProps.put("retries", "0")
  producerProps.put("linger.ms", "0")
  producerProps.put("key.serializer", classOf[ByteArraySerializer].getName)
  producerProps.put("value.serializer", classOf[ByteArraySerializer].getName)
  val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

  println(s"KafkaBackedMemStore Ready for kafka topic: `$topic`, partition: $partition")

  def write(kv: (Array[Byte], Array[Byte])) = {
    producer.send(new ProducerRecord(topic, partition, kv._1, kv._2))
  }



}


package io.amient.affinity.example.graphapi.actor

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import io.amient.affinity.core.storage.{MemStoreSimpleMap, Storage}
import io.amient.affinity.example.graphapi.actor.LocalHandler.Component
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

import scala.collection.JavaConverters._

class PrototypeKafkaBackedMemStore(partition :Int)
  extends MemStoreSimpleMap[Int, Component]() with Storage[Int, Component] {
  final val topic = "graph"

  val v = Component(1, "test", Set())
  val (sk, sv) = serialize(v.key, v)
  assert(deserialize(sk, sv)._2 == v)

  println(s"Bootstrapping memstore partition from kafka topic: `$topic`, partition: $partition")
  val consumerProps = new Properties()
  consumerProps.put("bootstrap.servers", "localhost:9092")
  consumerProps.put("enable.auto.commit", "false")
  consumerProps.put("key.deserializer", classOf[ByteArrayDeserializer].getName)
  consumerProps.put("value.deserializer", classOf[ByteArrayDeserializer].getName)

  val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
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
    for(r <- records.iterator().asScala) {
      val (key, value) = deserialize(r.key(), r.value())
      update(key, value)
    }
    if (consumer.position(tp) >= lastOffset) {
      //TODO if standby than continue until takes over from master
      //TODO multiple stores cannot block each other but have to join all together to become a master
      continue.set(false)
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

  override def serialize: (Int, Component) => (Array[Byte], Array[Byte]) = (k, v) => {
    val keyBytes = new ByteArrayOutputStream()
    val keyOut = new ObjectOutputStream(keyBytes)
    keyOut.writeObject(k)
    keyOut.close()
    val valueBytes = new ByteArrayOutputStream()
    val valueOut = new ObjectOutputStream(valueBytes)
    valueOut.writeObject(v)
    valueOut.close()
    (keyBytes.toByteArray, valueBytes.toByteArray)
  }

  override def deserialize: (Array[Byte], Array[Byte]) => (Int, Component) = (k, v) => {
    val keyBytes = new ByteArrayInputStream(k)
    val keyIn = new ObjectInputStream(keyBytes)
    val key = keyIn.readObject().asInstanceOf[Int]
    keyIn.close()
    val valueBytes = new ByteArrayInputStream(v)
    val valueIn = new ObjectInputStream(valueBytes)
    val value = valueIn.readObject().asInstanceOf[Component]
    valueIn.close()
    (key, value)
  }

}


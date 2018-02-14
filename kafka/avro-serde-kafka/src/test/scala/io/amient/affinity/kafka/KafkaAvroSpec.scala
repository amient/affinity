package io.amient.affinity.kafka

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.ConfluentSchemaRegistry
import io.amient.affinity.avro.ConfluentSchemaRegistry.CfAvroConf
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{FlatSpec, Matchers, Suite}

import scala.collection.JavaConversions._

case class TestRecord(key: Int, ts: Long = 0L, text: String = "") extends AvroRecord {
  override def hashCode(): Int = key.hashCode()
}

class KafkaAvroSpec extends FlatSpec with Suite
  with EmbeddedZooKeeper with EmbeddedKafka with EmbeddedCfRegistry with Matchers {

  //TODO test produce with confluent registry serializer, read with affinity case classes

  override def numPartitions: Int = 1

  behavior of "KafkaAvroSerializer and KafkaAvroDeserializer"

  it should "write case classes with generic registry and confluent schema registry deserializer should read them as GenericRecords" in {

    val producerConfig = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "key.serializer" -> classOf[KafkaAvroSerializer].getName,
      "value.serializer" -> classOf[KafkaAvroSerializer].getName,
      new AvroConf().Class.path -> classOf[ConfluentSchemaRegistry].getName,
      new CfAvroConf().ConfluentSchemaRegistryUrl.path -> registryUrl
    )

    val consumerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "group.id" -> "group2",
      "auto.offset.reset" -> "earliest",
      "schema.registry.url" -> registryUrl,
      "key.deserializer" -> classOf[io.confluent.kafka.serializers.KafkaAvroDeserializer].getName,
      "value.deserializer" -> classOf[io.confluent.kafka.serializers.KafkaAvroDeserializer].getName,
      "max.poll.records" -> 1000
    )

    val producer = new KafkaProducer[Int, TestRecord](producerConfig)

    val topic = "test"
    val numWrites = new AtomicInteger(0)
    for (i <- (1 to 10)) {
      producer.send(new ProducerRecord[Int, TestRecord](
        topic, i, TestRecord(i, System.currentTimeMillis(), s"test value $i"))).get
      numWrites.incrementAndGet
    }
    numWrites.get should be > (0)

    val consumer = new KafkaConsumer[Int, GenericRecord](
      consumerProps.mapValues(_.toString.asInstanceOf[AnyRef]))

    consumer.subscribe(List(topic))
    try {
      var read = 0
      while (read < numWrites.get) {
        val records = consumer.poll(10000)
        if (records.isEmpty) throw new Exception("Consumer poll timeout")
        for (record <- records) {
          read += 1
          record.value.get("key") should equal(record.key)
          record.value.get("text").asInstanceOf[Utf8].toString should equal(s"test value ${record.key}")
        }
      }
    } finally {
      consumer.close()
    }
  }

  it should "write case classes via pre-configured confluent registry and read with affinity deserializer" in {

    object TestRegistry extends ConfluentSchemaRegistry(ConfigFactory.defaultReference
      .withValue(new CfAvroConf().ConfluentSchemaRegistryUrl.path, ConfigValueFactory.fromAnyRef(registryUrl))) {
      register[TestRecord]
    }

    val topic = "test"
    val numWrites = new AtomicInteger(0)

    val producer = new KafkaProducer[Int, TestRecord](Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "key.serializer" -> classOf[KafkaAvroSerializer].getName,
      "value.serializer" -> classOf[KafkaAvroSerializer].getName,
      new AvroConf().Class.path -> classOf[ConfluentSchemaRegistry].getName,
      new CfAvroConf().ConfluentSchemaRegistryUrl.path -> registryUrl
    ).mapValues(_.asInstanceOf[AnyRef]))

    val updates = for (i <- (1 to 10)) yield {
      producer.send(new ProducerRecord[Int, TestRecord](
        topic, i, TestRecord(i, System.currentTimeMillis(), s"test value $i"))).get
      numWrites.incrementAndGet
    }

    val consumerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "group.id" -> "group2",
      "auto.offset.reset" -> "earliest",
      "max.poll.records" -> 1000,
      "key.deserializer" -> classOf[KafkaAvroDeserializer].getName,
      "value.deserializer" -> classOf[KafkaAvroDeserializer].getName,
      new AvroConf().Class.path -> classOf[ConfluentSchemaRegistry].getName,
      new CfAvroConf().ConfluentSchemaRegistryUrl.path -> registryUrl

    )

    val consumer = new KafkaConsumer[Int, TestRecord](consumerProps.mapValues(_.toString.asInstanceOf[AnyRef]))

    consumer.subscribe(List(topic))
    try {

      var read = 0
      while (read < numWrites.get) {
        val records = consumer.poll(10000)
        if (records.isEmpty) throw new Exception("Consumer poll timeout")
        for (record <- records) {
          read += 1
          record.value.key should equal(record.key)
          record.value.text should equal(s"test value ${record.key}")
        }
      }
    } finally {
      consumer.close()
    }


  }

}
package io.amient.affinity.kafka

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.schema.CfAvroSchemaRegistry
import io.amient.affinity.avro.{AvroRecord, AvroSerde}
import io.amient.affinity.testutil.EmbeddedZooKeeper
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{FlatSpec, Matchers, Suite}

import scala.collection.JavaConversions._

case class TestRecord(key: Int, ts: Long = 0L, text: String = "") extends AvroRecord[TestRecord] {
  override def hashCode(): Int = key.hashCode()
}

class KafkaAvroSerdeSpec extends FlatSpec with Suite
  with EmbeddedZooKeeper with EmbeddedKafka with EmbeddedCfRegistry with Matchers {

  //TODO test produce with confluent registry serializer, read with affinity case classes

  override def numPartitions: Int = 1

  behavior of "KafkaSerializer"

  it should "write case classes with generic registry and confluent schema registry deserializer should read them as GenericRecords" in {

    val registry: AvroSerde = AvroSerde.create(ConfigFactory.defaultReference
      .withValue(AvroSerde.CONFIG_PROVIDER_CLASS, ConfigValueFactory.fromAnyRef(classOf[CfAvroSchemaRegistry].getName))
      .withValue(CfAvroSchemaRegistry.CONFIG_CF_REGISTRY_URL_BASE, ConfigValueFactory.fromAnyRef(registryUrl)))

    registry should not be (null)

    val topic = "test"
    val numWrites = new AtomicInteger(0)

    val producer = new KafkaProducer[Int, TestRecord](
      Map("bootstrap.servers" -> kafkaBootstrap).mapValues(_.toString.asInstanceOf[AnyRef]),
      KafkaSerializer[Int](registry),
      KafkaSerializer[TestRecord](registry)
    )
    val updates = for (i <- (1 to 10)) yield {
      producer.send(new ProducerRecord[Int, TestRecord](
        topic, i, TestRecord(i, System.currentTimeMillis(), s"test value $i"))).get
      numWrites.incrementAndGet
    }
    numWrites.get should be > (0)

    val consumerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "group.id" -> "group2",
      "auto.offset.reset" -> "earliest",
      "schema.registry.url" -> registryUrl,
      "key.deserializer" -> classOf[io.confluent.kafka.serializers.KafkaAvroDeserializer].getName,
      "value.deserializer" -> classOf[io.confluent.kafka.serializers.KafkaAvroDeserializer].getName,
      "max.poll.records" -> 1000
    )

    val consumer = new KafkaConsumer[Int, GenericRecord](
      consumerProps.mapValues(_.toString.asInstanceOf[AnyRef]))

    consumer.subscribe(List(topic))
    try {

      var read = 0
      while (read < numWrites.get) {
        val records = consumer.poll(1000)
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

  it should "work with specific registry" in {

    object TestRegistry extends CfAvroSchemaRegistry(ConfigFactory
      .defaultReference.withValue(CfAvroSchemaRegistry.CONFIG_CF_REGISTRY_URL_BASE, ConfigValueFactory.fromAnyRef(registryUrl))) {
      register(classOf[Boolean])
      register(classOf[Int])
      register(classOf[Long])
      register(classOf[Float])
      register(classOf[Double])
      register(classOf[String])
      register(classOf[Null])
      register(classOf[TestRecord])
      initialize()
    }

    val topic = "test"
    val numWrites = new AtomicInteger(0)

    val producer = new KafkaProducer[Int, TestRecord](
      Map("bootstrap.servers" -> kafkaBootstrap).mapValues(_.toString.asInstanceOf[AnyRef]),
      KafkaSerializer[Int](TestRegistry),
      KafkaSerializer[TestRecord](TestRegistry)
    )

    val updates = for (i <- (1 to 10)) yield {
      producer.send(new ProducerRecord[Int, TestRecord](
        topic, i, TestRecord(i, System.currentTimeMillis(), s"test value $i"))).get
      numWrites.incrementAndGet
    }

    val consumerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "group.id" -> "group2",
      "auto.offset.reset" -> "earliest",
      "max.poll.records" -> 1000
    )

    val consumer = new KafkaConsumer[Int, TestRecord](
      consumerProps.mapValues(_.toString.asInstanceOf[AnyRef]),
      KafkaDeserializer[Int](TestRegistry),
      KafkaDeserializer[TestRecord](TestRegistry))

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
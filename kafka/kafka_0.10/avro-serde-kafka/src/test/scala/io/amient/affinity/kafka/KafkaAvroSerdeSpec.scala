package io.amient.affinity.kafka

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.schema.ZkAvroSchemaRegistry
import io.amient.affinity.avro.{AvroRecord, AvroSerde}
import io.amient.affinity.core.util.ByteUtils
import io.amient.affinity.testutil.EmbeddedZooKeeper
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.scalatest.{FlatSpec, Matchers, Suite}

import scala.collection.JavaConversions._

object UUID {
  def apply(uuid: java.util.UUID): UUID = apply(ByteBuffer.wrap(ByteUtils.uuid(uuid)))

  def random: UUID = apply(java.util.UUID.randomUUID)
}

case class UUID(val data: ByteBuffer) extends AvroRecord[UUID] {
  def javaUUID: java.util.UUID = ByteUtils.uuid(data.array)
}

case class TestRecord(key: Int, uuid: UUID, ts: Long = 0L, text: String = "") extends AvroRecord[TestRecord] {
  override def hashCode(): Int = key.hashCode()
}


class KafkaAvroSerdeSpec extends FlatSpec with Suite with EmbeddedZooKeeper with EmbeddedKafka with Matchers {

  //TODO test 1.: produce with plain case class and generic registry (without pre-registration)
  //TODO test 2.: produce with affinity case classes, read with affinity case clases
  //TODO test 3.: produce with affinity case classes, read with confluent registry deserializer
  //TODO test 4.: produce with confluent registry serializer, read with affinity case classes

  override def numPartitions: Int = 2

  behavior of "KafkaSerializer"

  it should "work with generic registry" in {

    val topic = "test"
    val numWrites = new AtomicInteger(0)

    val registry: AvroSerde = AvroSerde.create(ConfigFactory.defaultReference
      .withValue(AvroSerde.CONFIG_PROVIDER_CLASS, ConfigValueFactory.fromAnyRef(classOf[ZkAvroSchemaRegistry].getName))
      .withValue(ZkAvroSchemaRegistry.CONFIG_ZOOKEEPER_CONNECT, ConfigValueFactory.fromAnyRef(zkConnect)))

    registry should not be (null)

    val consumerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "group.id" -> "group2",
      "auto.offset.reset" -> "earliest",
      "max.poll.records" -> 1000
    )

    val consumer = new KafkaConsumer[Int, TestRecord](
      consumerProps.mapValues(_.toString.asInstanceOf[AnyRef]),
      KafkaDeserializer[Int](registry),
      KafkaDeserializer[TestRecord](registry))

    consumer.subscribe(List(topic))
    try {

      var read = 0
      val numReads = numWrites.get
      while (read < numReads) {
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

  it should "work with specific registry" in {

    //trait TestRegistry(config: Config) CfAvroSchemaRegistry(config) {
    //  self: AvroSchemaProvider =>
    //  register(classOf[Boolean])
    //  register(classOf[Int])
    //  register(classOf[Long])
    //  register(classOf[Float])
    //  register(classOf[Double])
    //  register(classOf[String])
    //  register(classOf[Null])
    //  register(classOf[UUID])
    //  register(classOf[TestRecord])
    //}

    //    val registry: AvroSerde = new TestZkAvroRegistry(ConfigFactory.empty
    //      .withValue(ZkAvroSchemaRegistry.CONFIG_ZOOKEEPER_CONNECT, ConfigValueFactory.fromAnyRef(zkConnect)))

  }

}
package io.amient.affinity.kafka

import java.nio.ByteBuffer

import com.typesafe.config.Config
import io.amient.affinity.avro.AvroRecord
import io.amient.affinity.avro.schema.{AvroSchemaProvider, CfAvroSchemaRegistry, ZkAvroSchemaRegistry}
import io.amient.affinity.core.util.ByteUtils
import io.amient.affinity.testutil.EmbeddedZooKeeper
import org.scalatest.{FlatSpec, Suite}

object UUID {
  def apply(uuid: java.util.UUID): UUID = apply(ByteBuffer.wrap(ByteUtils.uuid(uuid)))

  def random: UUID = apply(java.util.UUID.randomUUID)
}

case class UUID(val data: ByteBuffer) extends AvroRecord[UUID] {
  def javaUUID: java.util.UUID = ByteUtils.uuid(data.array)
}

case class KEY(id: Int) extends AvroRecord[KEY] {
  override def hashCode(): Int = id.hashCode()
}

case class TestRecord(key: KEY, uuid: UUID, ts: Long = 0L, text: String = "") extends AvroRecord[TestRecord] {
  override def hashCode(): Int = key.hashCode()
}

trait TestRegistry {
  self: AvroSchemaProvider =>
  register(classOf[Boolean])
  register(classOf[Int])
  register(classOf[Long])
  register(classOf[Float])
  register(classOf[Double])
  register(classOf[String])
  register(classOf[Null])
  register(classOf[UUID])
  register(classOf[TestRecord])
}

class TestAvroRegistry(config: Config) extends CfAvroSchemaRegistry(config) with TestRegistry

class TestZkAvroRegistry(config: Config) extends ZkAvroSchemaRegistry(config) with TestRegistry


class KafkaAvroSerdeSpec extends FlatSpec with Suite with EmbeddedZooKeeper with EmbeddedKafka {

  override def numPartitions: Int = 2

  behavior of "KafkaDeserializer"

  it should "user avro for primitive keys and complex values" in {

    val consumerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "group.id" -> "group2",
      "auto.offset.reset" -> "earliest",
      "max.poll.records" -> 1000
    )

//    val registry: AvroSerde = AvroSerde.create(system.settings.config)
//    val consumer = new KafkaConsumer[Int, TestRecord](
//      consumerProps.mapValues(_.toString.asInstanceOf[AnyRef]).asJava,
//      KafkaDeserializer[Int](registry),
//      KafkaDeserializer[TestRecord](registry))
//
//    consumer.subscribe(List(topic).asJava)
//    try {
//
//      var read = 0
//      val numReads = numWrites.get
//      while (read < numReads) {
//        val records = consumer.poll(10000)
//        if (records.isEmpty) throw new Exception("Consumer poll timeout")
//        for (record <- records.asScala) {
//          read += 1
//          record.value.key.id should equal(record.key)
//          record.value.text should equal(s"test value ${record.key}")
//        }
//      }
//    } finally {
//      consumer.close()
//    }
//

  }
}

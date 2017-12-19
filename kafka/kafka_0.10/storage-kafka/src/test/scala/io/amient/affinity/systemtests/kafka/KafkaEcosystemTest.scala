package io.amient.affinity.systemtests.kafka

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.AvroSerde.AvroConf
import io.amient.affinity.avro.schema.ZkAvroSchemaRegistry
import io.amient.affinity.avro.schema.ZkAvroSchemaRegistry.ZkAvroConf
import io.amient.affinity.avro.{AvroRecord, AvroSerde}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.core.util.{ByteUtils, SystemTestBase}
import io.amient.affinity.kafka.{EmbeddedKafka, KafkaAvroDeserializer}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object UUID {
  def apply(uuid: java.util.UUID): UUID = apply(ByteBuffer.wrap(ByteUtils.uuid(uuid)))

  def random: UUID = apply(java.util.UUID.randomUUID)
}

case class UUID(val data: ByteBuffer) extends AvroRecord {
  def javaUUID: java.util.UUID = ByteUtils.uuid(data.array)
}

case class KEY(id: Int) extends AvroRecord {
  override def hashCode(): Int = id.hashCode()
}

case class TestRecord(key: KEY, uuid: UUID, ts: Long = 0L, text: String = "") extends AvroRecord {
  override def hashCode(): Int = key.hashCode()
}

class KafkaEcosystemTest extends FlatSpec with SystemTestBase with EmbeddedKafka with Matchers {

  override def numPartitions: Int = 2

  private val log = LoggerFactory.getLogger(classOf[KafkaEcosystemTest])

  val config = configure(ConfigFactory.load("systemtests")
    .withValue(new AvroSerde.Conf().Avro.Class.path, ConfigValueFactory.fromAnyRef(classOf[ZkAvroSchemaRegistry].getName))
    , Some(zkConnect), Some(kafkaBootstrap))

  val system = ActorSystem.create("KafkaEcoSystem", config)

  import system.dispatcher

  override def beforeAll: Unit = {
    SerializationExtension(system)
  }

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private def createStateStoreForPartition(name: String)(implicit keyspace: String, partition: Int) = {
    State.create[Int, TestRecord](keyspace, partition, name, system)
  }

  behavior of "KafkaDeserializer"

  it should "be able to work with ZkAvroSchemaRegistry" in {
    config.getString(new AvroSerde.Conf().Avro.Class.path) should be (classOf[ZkAvroSchemaRegistry].getName)
    system.settings.config.getString(new AvroSerde.Conf().Avro.Class.path) should be (classOf[ZkAvroSchemaRegistry].getName)

    val stateStoreName = "throughput-test"
    val topic = KafkaStorage.StateConf(Node.Conf(config).Affi.Keyspace("keyspace1").State(stateStoreName)).Storage.Topic()
    val state = createStateStoreForPartition(stateStoreName)("keyspace1", partition = 0)
    val numWrites = new AtomicInteger(10)
    val numToWrite = numWrites.get
    val l = System.currentTimeMillis()
    val updates = Future.sequence(for (i <- (1 to numToWrite)) yield {
      state.replace(i, TestRecord(KEY(i), UUID.random, System.currentTimeMillis(), s"test value $i")) transform(
        (s) => s, (e: Throwable) => {
        numWrites.decrementAndGet()
        e
      })
    })
    Await.ready(updates, 10 seconds)
    log.info(s"written ${numWrites.get} records of state data in ${System.currentTimeMillis() - l} ms")
    state.numKeys should equal(numWrites.get)

    val consumerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "group.id" -> "group2",
      "auto.offset.reset" -> "earliest",
      "max.poll.records" -> 1000,
      "key.deserializer" -> classOf[KafkaAvroDeserializer].getName,
      "value.deserializer" -> classOf[KafkaAvroDeserializer].getName,
      new AvroConf().Class.path -> classOf[ZkAvroSchemaRegistry].getName,
      new ZkAvroConf().Connect.path -> zkConnect
    )

    val consumer = new KafkaConsumer[Int, TestRecord](consumerProps.mapValues(_.toString.asInstanceOf[AnyRef]))

    consumer.subscribe(List(topic))
    try {

      var read = 0
      val numReads = numWrites.get
      while (read < numReads) {
        val records = consumer.poll(10000)
        if (records.isEmpty) throw new Exception("Consumer poll timeout")
        for (record <- records) {
          read += 1
          record.value.key.id should equal(record.key)
          record.value.text should equal(s"test value ${record.key}")
        }
      }
    } finally {
      consumer.close()
    }


  }
}

package io.amient.affinity.systemtests.core

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.serialization.SerializationExtension
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.serde.avro.AvroRecord
import io.amient.affinity.core.serde.avro.schema.CfAvroSchemaRegistry
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.testutil.SystemTestBaseWithConfluentRegistry
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.IndexedRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

//TODO #16 support basic avro types not just AvroRecord val id = registryClient/.register("topic-key", SchemaBuilder.builder().intType())
case class INT(i: Int) extends AvroRecord[INT]

case class TestRecord(id: INT, ts: Long = 0L, value: String = "") extends AvroRecord[TestRecord]

class TestAvroRegistry(system: ExtendedActorSystem) extends CfAvroSchemaRegistry(system) {
  register(classOf[INT])
  register(classOf[TestRecord])
}

class ConfluentEcoSystemTest extends FlatSpec with SystemTestBaseWithConfluentRegistry with Matchers {

  val config = configure {
    ConfigFactory.load("systemtests")
      .withValue("akka.actor.serializers.avro", ConfigValueFactory.fromAnyRef(classOf[TestAvroRegistry].getName))
  }

  val system = ActorSystem.create("ConfluentEcoSystem", config)

  import system.dispatcher

  override def beforeAll: Unit = {
    SerializationExtension(system)
  }

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  "AvroRecords registered with Affinity" should "be visible to the Confluent Registry Client" in {
    val testRecordId = registryClient.getLatestSchemaMetadata(classOf[TestRecord].getName).getId
    registryClient.getByID(testRecordId) should equal(AvroRecord.inferSchema(classOf[TestRecord]))
    val intId = registryClient.getLatestSchemaMetadata(classOf[INT].getName).getId
    registryClient.getByID(intId) should equal(AvroRecord.inferSchema(classOf[INT]))
  }

  "Confluent KafkaAvroDeserializer" should "read Affinity AvroRecords as IndexedRecords (high throughput scenario)" in {
    test("throughput-test")
  }

  "Confluent KafkaAvroDeserializer" should "read Affinity AvroRecords as IndexedRecords (failing writes scenario)" in {
    test("failure-test")
  }

  private def test(stateStoreName: String) {
    implicit val partition = 0
    val stateStoreConfig = config.getConfig(State.CONFIG_STATE_STORE(stateStoreName))
    val state = new State[INT, TestRecord](system, stateStoreConfig)
    println(s"kafka available at zookeeper connection $zkConnect")
    val numWrites = new AtomicInteger(5000)
    val numToWrite = numWrites.get
    val l = System.currentTimeMillis()
    val updates = Future.sequence(for (i <- (1 to numToWrite)) yield {
      state.put(INT(i), TestRecord(INT(i), System.currentTimeMillis(), s"test value $i")) transform (
        (s) => s, (e: Throwable) => { numWrites.decrementAndGet(); e })
    })
    Await.ready(updates, 10 seconds)
    println(s"written ${numWrites.get} records of state data in ${System.currentTimeMillis() - l} ms")
    state.size should equal(numWrites.get)

    val props = new Properties()
    props.put("bootstrap.servers", kafkaBootstrap)
    props.put("group.id", "group2")
    props.put("auto.offset.reset", "earliest")
    props.put("max.poll.records", "1000")
    props.put("schema.registry.url", registryUrl)
    props.put("key.deserializer", classOf[KafkaAvroDeserializer].getName)
    props.put("value.deserializer", classOf[KafkaAvroDeserializer].getName)

    val consumer = new KafkaConsumer[IndexedRecord, IndexedRecord](props)
    consumer.subscribe(List(stateStoreConfig.getString(KafkaStorage.CONFIG_KAFKA_TOPIC)).asJava)
    try {

      var read = 0
      val numReads = numWrites.get
      while (read < numReads) {
        val records = consumer.poll(1000)
        if (records.isEmpty) throw new Exception("Consumer poll timeout")
        for (record <- records.asScala) {
          read += 1
          //TODO #16 provide an efficient KafkaAvroSerde either as a wrapper or from scratch
          val intKey = INT(record.key.get(0).asInstanceOf[Int])
          AvroRecord.read[INT](record.key) should equal(intKey)
          AvroRecord.read[TestRecord](record.value) should equal {
            TestRecord(intKey, record.value.get(1).asInstanceOf[Long], s"test value ${intKey.i}")
          }
        }
      }
    } finally {
      consumer.close()
    }

  }

}



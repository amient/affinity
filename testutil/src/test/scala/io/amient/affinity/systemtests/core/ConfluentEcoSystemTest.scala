package io.amient.affinity.systemtests.core

import java.util.Properties

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.serialization.SerializationExtension
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.serde.avro.AvroRecord
import io.amient.affinity.core.serde.avro.schema.CfAvroSchemaRegistry
import io.amient.affinity.core.storage.State
import io.amient.affinity.testutil.SystemTestBaseWithConfluentRegistry
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.IndexedRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

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

  "Confluent KafkaAvroDeserializer" should "read Affinity AvroRecords as IndexedRecords" in {
    val topic = "test"

    implicit val partition = 0
    val state = new State[INT, TestRecord](system, config.getConfig(State.CONFIG_STATE_STORE(topic)))
    println(s"kafka available at zookeeper connection $zkConnect")
    val numWrites = 100
    //TODO #16 profile with numWrites = 1000000
    for (i <- (1 to numWrites)) {
      state.put(INT(i), Some(TestRecord(INT(i), System.currentTimeMillis(), s"test value $i")))
    }
    println(s"written $numWrites records of state data")

    val props = new Properties()
    props.put("bootstrap.servers", kafkaBootstrap)
    props.put("group.id", "group1")
    props.put("auto.offset.reset", "earliest")
    props.put("max.poll.records", "1000")
    props.put("schema.registry.url", registryUrl)
    props.put("key.deserializer", classOf[KafkaAvroDeserializer].getName)
    props.put("value.deserializer", classOf[KafkaAvroDeserializer].getName)

    val consumer = new KafkaConsumer[IndexedRecord, IndexedRecord](props)
    consumer.subscribe(List(topic).asJava)

    var read = 0
    while (read < numWrites) {
      val records = consumer.poll(1000)
      if (records.isEmpty) throw new Exception("Consumer poll timeout")
      for (record <- records.asScala) {
        read += 1
        //TODO #16 provide an efficient KafkaAvroSerde either as a wrapper or from scratch
        AvroRecord.read[INT](record.key) should equal {
          INT(read)
        }
        AvroRecord.read[TestRecord](record.value) should equal {
          TestRecord(INT(read), record.value.get(1).asInstanceOf[Long], s"test value $read")
        }
      }
    }


  }

}



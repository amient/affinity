package io.amient.affinity.avro

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.Conf
import io.amient.affinity.avro.ConfluentSchemaRegistry.CfAvroConf
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.kafka.EmbeddedConfluentRegistry
import org.scalatest.FlatSpec


case class ExampleType(val id: Int) extends AvroRecord {
  override def hashCode(): Int = id.hashCode()
}

class AkkaSerializationSystemTest extends FlatSpec with EmbeddedConfluentRegistry {

  val config = ConfigFactory.defaultReference
    .withValue(Conf.Affi.Avro.Class.path, ConfigValueFactory.fromAnyRef(classOf[ConfluentSchemaRegistry].getName))
    .withValue(CfAvroConf(Conf.Affi.Avro).ConfluentSchemaRegistryUrl.path, ConfigValueFactory.fromAnyRef(registryUrl))


  assert(config.getString(CfAvroConf(Conf.Affi.Avro).ConfluentSchemaRegistryUrl.path) == registryUrl)

  override def numPartitions = 2

  "Confluent Schema Registry " should "be available via akka SerializationExtension" in {
    val system = ActorSystem.create("CfTest", config)
    try {
      val serialization = SerializationExtension(system)
      val serde = serialization.serializerFor(classOf[ExampleType])
      assert(serde.fromBinary(serde.toBinary(ExampleType(101))) == ExampleType(101))
    } finally {
      system.terminate()
    }
  }
}

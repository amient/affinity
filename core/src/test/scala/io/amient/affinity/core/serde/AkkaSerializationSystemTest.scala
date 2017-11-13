package io.amient.affinity.core.serde

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.schema.CfAvroSchemaRegistry
import io.amient.affinity.avro.{AvroRecord, AvroSerde}
import io.amient.affinity.kafka.EmbeddedCfRegistry
import io.amient.affinity.testutil.SystemTestBase
import org.scalatest.FlatSpec


case class ExampleType(val id: Int) extends AvroRecord[ExampleType] {
  override def hashCode(): Int = id.hashCode()
}


class MyConfluentRegistry(config: Config) extends CfAvroSchemaRegistry(config) {
  register(classOf[ExampleType])
}

class AkkaSerializationSystemTest extends FlatSpec with SystemTestBase with EmbeddedCfRegistry {

  val config = configure(ConfigFactory.defaultReference)
    .withValue(CfAvroSchemaRegistry.CONFIG_CF_REGISTRY_URL_BASE, ConfigValueFactory.fromAnyRef(registryUrl))
    .withValue(AvroSerde.CONFIG_PROVIDER_CLASS, ConfigValueFactory.fromAnyRef(classOf[MyConfluentRegistry].getName))

  assert(config.getString(CfAvroSchemaRegistry.CONFIG_CF_REGISTRY_URL_BASE) == registryUrl)

  override def numPartitions = 2

  "Confluent Schema Registry Provider" should "be available via akka SerializationExtension" in {
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

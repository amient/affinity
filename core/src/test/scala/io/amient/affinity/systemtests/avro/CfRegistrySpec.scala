package io.amient.affinity.systemtests.avro

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.AvroSerde
import io.amient.affinity.core.serde.avro._
import io.amient.affinity.avro.schema.CfAvroSchemaRegistry
import io.amient.affinity.kafka.EmbeddedCfRegistry
import io.amient.affinity.testutil.{SystemTestBase, SystemTestBaseWithConfluentRegistry}
import org.scalatest.FlatSpec


class MyConfluentRegistry(config: Config) extends CfAvroSchemaRegistry(config) {
  register(classOf[ID])
  register(classOf[Base])
  register(classOf[Composite])
}

class CfRegistrySpec extends FlatSpec with SystemTestBase with EmbeddedCfRegistry {

  val config = configure(ConfigFactory.defaultReference)
    .withValue(CfAvroSchemaRegistry.CONFIG_CF_REGISTRY_URL_BASE, ConfigValueFactory.fromAnyRef(registryUrl))
    .withValue(AvroSerde.CONFIG_PROVIDER_CLASS, ConfigValueFactory.fromAnyRef(classOf[MyConfluentRegistry].getName))

  assert(config.getString(CfAvroSchemaRegistry.CONFIG_CF_REGISTRY_URL_BASE) == registryUrl)

  override def numPartitions = 2

  "Confluent Schema Registry Provider" should "be available via akka SerializationExtension" in {
    val system = ActorSystem.create("CfTest", config)
    try {
      val serialization = SerializationExtension(system)
      val serde = serialization.serializerFor(classOf[ID])
      assert(serde.fromBinary(serde.toBinary(ID(101))) == ID(101))
    } finally {
      system.terminate()
    }
  }
}

package io.amient.affinity.systemtests.avro

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.serialization.SerializationExtension
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.serde.avro.schema.CfAvroSchemaRegistry
import io.amient.affinity.core.serde.avro.{Base, Composite, ID}
import io.amient.affinity.testutil.SystemTestBaseWithConfluentRegistry
import org.scalatest.FlatSpec


class MyConfluentRegistry(system: ExtendedActorSystem) extends CfAvroSchemaRegistry(system) {
  register(classOf[ID])
  register(classOf[Base])
  register(classOf[Composite])
}

class CfRegistryTest extends FlatSpec with SystemTestBaseWithConfluentRegistry {

  val config = configure {
    ConfigFactory.defaultReference()
      .withValue("akka.actor.serializers.avro", ConfigValueFactory.fromAnyRef(classOf[MyConfluentRegistry].getName))
  }

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

package io.amient.affinity.core.serde.avro

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.serialization.SerializationExtension
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.serde.avro.schema.CfAvroSchemaRegistry
import io.amient.affinity.testutil.SystemTestBaseWithConfluentRegistry
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._

class MyConfluentRegistry(system: ExtendedActorSystem) extends CfAvroSchemaRegistry(system) {
  register(classOf[ID])
  register(classOf[Base])
  register(classOf[Composite])
}

class CfRegistryTest extends FlatSpec with SystemTestBaseWithConfluentRegistry {

  val config = configure { ConfigFactory.defaultReference()
      .withValue("akka.actor.serializers.avro", ConfigValueFactory.fromAnyRef(classOf[MyConfluentRegistry].getName))
      .withValue("akka.actor.serialization-bindings", ConfigValueFactory.fromMap(Map(
        "io.amient.affinity.core.serde.avro.AvroRecord" -> "avro"
      ).asJava))
  }

  "Confluent Schema Registry Provider" should "be available via akk SerializationExtension" in {
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

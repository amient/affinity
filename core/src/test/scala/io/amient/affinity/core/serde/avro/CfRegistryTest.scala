package io.amient.affinity.core.serde.avro

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.serialization.SerializationExtension
import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.serde.avro.schema.{AvroSchemaProvider, CfAvroSchemaRegistry}


class MyConfluentRegistry(system: ExtendedActorSystem) extends CfAvroSchemaRegistry(system) {
  register(classOf[ID])
  register(classOf[Base])
  register(classOf[Composite])
}

/**
  * This is not a unit test but a manual system test application which requires a Confluent Schema Registry
  * to be running on port 7011.
  */

object CfRegistryTest extends App {

  val system = ActorSystem.create("CfTest", ConfigFactory.load("cftest"))
  try {
    val serialization = SerializationExtension(system)
    val serde = serialization.serializerFor(classOf[ID])
    assert(serde.fromBinary(serde.toBinary(ID(101))) == ID(101))
  } finally {
    system.terminate()
  }
}

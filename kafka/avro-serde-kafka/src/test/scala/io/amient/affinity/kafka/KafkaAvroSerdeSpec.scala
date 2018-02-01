package io.amient.affinity.kafka

import java.util

import io.amient.affinity.avro.{ConfluentSchemaRegistry, MemorySchemaRegistry}
import org.scalatest.{FlatSpec, Matchers}

class KafkaAvroSerdeSpec extends FlatSpec with Matchers {

  behavior of "KafkaAvroSerde for Kafka Streams"

  it should "work with a SchemaRegistry"  in {
    val serde = new SpecificKafkaAvroSerde[SimpleKey]
    serde.configure(new util.HashMap[String,String] {
      put("schema.registry.class", classOf[MemorySchemaRegistry].getName)
      put("schema.registry.id", "1")
    }, true)
    val value = SimpleKey(1)
    val x: Array[Byte] = serde.serializer.serialize("test", value)
    serde.deserializer.deserialize("test", x) should be(value)
  }

}

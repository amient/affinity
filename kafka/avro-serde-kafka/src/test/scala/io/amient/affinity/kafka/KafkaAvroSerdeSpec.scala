package io.amient.affinity.kafka

import java.util

import io.amient.affinity.avro.{ConfluentSchemaRegistry, MemorySchemaRegistry}
import org.scalatest.{FlatSpec, Matchers}

class KafkaAvroSerdeSpec extends FlatSpec with Matchers with EmbeddedCfRegistry {

  behavior of "KafkaAvroSerde for Kafka Streams"

  override def numPartitions: Int = 1

  it should "work with Confluent Schema Registry"  in {
    val serde = new KafkaAvroSerde[SimpleKey]
    serde.configure(new util.HashMap[String,String] {
      put("schema.registry.class", classOf[ConfluentSchemaRegistry].getName)
      put("schema.registry.url", registryUrl)
    }, true)
    val value = SimpleKey(1)
    val x: Array[Byte] = serde.serializer.serialize("test", value)
    serde.deserializer.deserialize("test", x) should be(value)
  }

  it should "work with MemorySchemaRegistry"  ignore {
    //FIXME MemorySchemaRegistry doesn't take any configuration so cannot be shared accress serializer - deserializer
    val serde = new KafkaAvroSerde[SimpleKey]
    serde.configure(new util.HashMap[String,String] {
      put("schema.registry.class", classOf[MemorySchemaRegistry].getName)
    }, true)
    val value = SimpleKey(1)
    val x: Array[Byte] = serde.serializer.serialize("test", value)
    serde.deserializer.deserialize("test", x) should be(value)
  }

}

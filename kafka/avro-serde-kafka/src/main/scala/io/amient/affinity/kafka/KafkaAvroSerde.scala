package io.amient.affinity.kafka

import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

class KafkaAvroSerde[T <: SpecificRecord] extends Serde[T] {
  val innerDeserializer = new TypedKafkaAvroDeserializer[T]
  val innerSerializer = new TypedKafkaAvroSerializer[T]

  override def deserializer(): Deserializer[T] = innerDeserializer

  override def serializer(): Serializer[T] = innerSerializer

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {
    innerDeserializer.configure(configs, isKey)
    innerSerializer.configure(configs, isKey)
  }

  override def close(): Unit = {
    innerDeserializer.close()
    innerSerializer.close()
  }
}

class TypedKafkaAvroSerializer[T <: SpecificRecord] extends Serializer[T] {
  val inner = new KafkaAvroSerializer

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = inner.configure(configs, isKey)

  override def serialize(topic: String, data: T): Array[Byte] = inner.serialize(topic, data)

  override def close(): Unit = inner.close()
}

class TypedKafkaAvroDeserializer[T <: SpecificRecord] extends Deserializer[T] {
  val inner = new KafkaAvroDeserializer

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = inner.configure(configs, isKey)

  override def deserialize(topic: String, data: Array[Byte]): T = inner.deserialize(topic, data).asInstanceOf[T]

  override def close(): Unit = inner.close()
}

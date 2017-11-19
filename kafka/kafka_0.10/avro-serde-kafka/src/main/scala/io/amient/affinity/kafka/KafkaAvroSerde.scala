package io.amient.affinity.kafka

import java.util

import com.typesafe.config.Config
import io.amient.affinity.avro.AvroSerde
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import scala.reflect.runtime.universe._

abstract class KafkaAvroSerde[T: TypeTag](serde: AvroSerde) {

  require(serde != null, "null serde provided")

  private var registered = false

  def ensureRegistered(topic: String, isKey: Boolean): Unit = {
    if (!registered) {
      serde.register[T]
      serde.register[T](s"$topic-${if (isKey) "key" else "value"}")
      serde.initialize()
      registered = true
    }
  }

}

object KafkaAvroSerde {

  def key[T: TypeTag](config: Config): Serializer[T] with Deserializer[T] = key[T](AvroSerde.create(config))

  def key[T: TypeTag](serde: AvroSerde): Serializer[T] with Deserializer[T] = {
    new KafkaAvroSerde[T](serde) with Serializer[T]  with Deserializer[T] {

      override def configure(configs: util.Map[String, _], isKey: Boolean) = require(isKey)

      override def close() = serde.close()

      override def serialize(topic: String, data: T) = {
        ensureRegistered(topic, isKey = true)
        serde.toBytes(data)
      }

      override def deserialize(topic: String, data: Array[Byte]) = {
        ensureRegistered(topic, isKey = true)
        serde.fromBytes(data).asInstanceOf[T]
      }
    }
  }

  def value[T: TypeTag](config: Config): Serializer[T] with Deserializer[T] = value[T](AvroSerde.create(config))

  def value[T: TypeTag](serde: AvroSerde): Serializer[T] with Deserializer[T] = {
    new KafkaAvroSerde[T](serde) with Serializer[T]  with Deserializer[T] {

      override def configure(configs: util.Map[String, _], isKey: Boolean) = require(!isKey)

      override def close() = serde.close()

      override def serialize(topic: String, data: T) = {
        ensureRegistered(topic, isKey = false)
        serde.toBytes(data)
      }

      override def deserialize(topic: String, data: Array[Byte]) = {
        ensureRegistered(topic, isKey = false)
        serde.fromBytes(data).asInstanceOf[T]
      }
    }
  }
}
package io.amient.affinity.kafka

import java.util

import com.typesafe.config.Config
import io.amient.affinity.avro.AvroSerde
import io.amient.affinity.core.serde.AbstractSerde
import org.apache.kafka.common.serialization.Deserializer

object KafkaDeserializer {

  def apply[T](config: Config): Deserializer[T] = apply[T](AvroSerde.create(config))

  def apply[T](serde: AbstractSerde[_ >: T]) = new Deserializer[T] {

    require(serde != null, "null serde provided")

    override def configure(configs: util.Map[String, _], isKey: Boolean) = ()

    override def close() = serde.close()

    override def deserialize(topic: String, data: Array[Byte]) = serde.fromBytes(data).asInstanceOf[T]
  }
}

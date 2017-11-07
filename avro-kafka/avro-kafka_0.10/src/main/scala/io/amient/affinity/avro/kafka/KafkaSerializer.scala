package io.amient.affinity.avro.kafka

import java.util

import io.amient.affinity.core.serde.AbstractSerde
import org.apache.kafka.common.serialization.{Serializer}

object KafkaSerializer {
  def apply[T](serde: AbstractSerde[_ >: T]) = new Serializer[T] {
    override def configure(configs: util.Map[String, _], isKey: Boolean) = ()

    override def close() = serde.close()

    override def serialize(topic: String, data: T) = serde.toBytes(data)
  }
}


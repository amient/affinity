package io.amient.affinity.kafka

import io.amient.affinity.avro.record.AvroRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.reflect.runtime.universe._

/**
  * This Serde can be used for internal topics of processors. These schemas won't be registered
  * in the schema registry.
  *
  * @tparam T
  */
class InternalKafkaAvroSerde[T: TypeTag] extends Serde[T] {
  val schema = AvroRecord.inferSchema[T]
    override def configure(configs: java.util.Map[String, _], isKey: Boolean) = ()
    override def close() = ()

    override def deserializer() = new Deserializer[T] {
      override def configure(configs: java.util.Map[String, _], isKey: Boolean) = ()
      override def close() = ()
      override def deserialize(topic: String, data: Array[Byte]) = AvroRecord.read(data, schema)
    }

    override def serializer() = new Serializer[T] {
      override def configure(configs: java.util.Map[String, _], isKey: Boolean) = ()
      override def close() = ()
      override def serialize(topic: String, data: T) = AvroRecord.write(data, schema)
    }

}

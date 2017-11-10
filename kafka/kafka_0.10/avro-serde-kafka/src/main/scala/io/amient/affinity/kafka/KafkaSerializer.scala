package io.amient.affinity.kafka

import java.util

import io.amient.affinity.avro.AvroSerde
import org.apache.kafka.common.serialization.Serializer

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object KafkaSerializer {
  def apply[T: TypeTag: ClassTag](serde: AvroSerde) = new Serializer[T] {

    val runtimeClass = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    if (serde.schema(runtimeClass.getCanonicalName).isEmpty) {
      serde.register[T](runtimeClass)
      serde.initialize()
      require(serde.schema(runtimeClass.getCanonicalName).isDefined)
    }

    override def configure(configs: util.Map[String, _], isKey: Boolean) = ()

    override def close() = serde.close()

    override def serialize(topic: String, data: T) = serde.toBytes(data)
  }
}


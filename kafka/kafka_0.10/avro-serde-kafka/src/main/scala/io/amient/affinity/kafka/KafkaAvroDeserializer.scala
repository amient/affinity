package io.amient.affinity.kafka

import java.util

import com.typesafe.config.Config
import io.amient.affinity.avro.AvroSerde
import org.apache.kafka.common.serialization.Deserializer

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object KafkaAvroDeserializer {

  def apply[T: TypeTag: ClassTag](config: Config): Deserializer[T] = apply[T](AvroSerde.create(config))

  def apply[T: TypeTag: ClassTag](serde: AvroSerde): Deserializer[T] = new Deserializer[T] {

    require(serde != null, "null serde provided")

    val runtimeClass = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    if (serde.schema(runtimeClass.getCanonicalName).isEmpty) {
      serde.register[T](runtimeClass)
      serde.initialize()
      require(serde.schema(runtimeClass.getCanonicalName).isDefined)
    }

    override def configure(configs: util.Map[String, _], isKey: Boolean) = ()

    override def close() = serde.close()

    override def deserialize(topic: String, data: Array[Byte]) = serde.fromBytes(data).asInstanceOf[T]
  }
}

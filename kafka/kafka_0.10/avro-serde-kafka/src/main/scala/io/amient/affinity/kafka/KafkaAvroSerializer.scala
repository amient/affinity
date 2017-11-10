package io.amient.affinity.kafka

import java.util

import com.typesafe.config.Config
import io.amient.affinity.avro.AvroSerde
import org.apache.kafka.common.serialization.Serializer

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object KafkaAvroSerializer {

  def apply[T: TypeTag: ClassTag](config: Config): Serializer[T] = apply[T](AvroSerde.create(config))

  def apply[T: TypeTag: ClassTag](serde: AvroSerde): Serializer[T] = new Serializer[T] {

    require(serde != null, "null serde provided")

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


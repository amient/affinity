package io.amient.affinity.kafka

import java.util

import com.typesafe.config.ConfigFactory
import io.amient.affinity.avro.{AvroRecord, AvroSerde}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.Serializer

class KafkaAvroSerializer extends Serializer[Any] {

  var isKey: Boolean = false
  var serde: AvroSerde = null

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    val config = ConfigFactory.parseMap(configs)
    this.serde = AvroSerde.create(config)
    this.isKey = isKey
  }

  override def serialize(topic: String, data: Any): Array[Byte] = {
    require(serde != null, "AvroSerde not configured")
    val subject = s"$topic-${if (isKey) "key" else "value"}"
    val objSchema = AvroRecord.inferSchema(data)
    val schemaId = serde.getCurrentSchema(objSchema.getFullName) match {
      case Some((schemaId: Int, regSchema: Schema)) if regSchema == objSchema => schemaId
      case _ =>
        serde.register(subject, objSchema)
        serde.register(objSchema.getFullName, objSchema)
        serde.initialize()
        serde.getSchemaId(objSchema) match {
          case None => throw new IllegalStateException(s"Failed to register schema for $subject")
          case Some(id) => id
        }
    }
    AvroRecord.write(data, objSchema, schemaId)
    serde.toBytes(data)
  }

  override def close(): Unit = if (serde != null) serde.close()
}

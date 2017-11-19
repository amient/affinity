package io.amient.affinity.kafka

import java.util

import com.typesafe.config.ConfigFactory
import io.amient.affinity.avro.{AvroRecord, AvroSerde}
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
    val schema = serde.getSchema(data)
    val schemaId: Int = serde.getSchemaId(schema) match  {
      case Some(id) => id
      case None =>
        serde.register(subject, schema)
        serde.register(schema.getFullName, schema)
        serde.initialize()
        serde.getSchemaId(schema) match {
          case None => throw new IllegalStateException(s"Failed to register schema for $subject")
          case Some(id) => id
        }
    }
    AvroRecord.write(data, schema, schemaId)
    serde.toBytes(data)
  }

  override def close(): Unit = if (serde != null) serde.close()
}

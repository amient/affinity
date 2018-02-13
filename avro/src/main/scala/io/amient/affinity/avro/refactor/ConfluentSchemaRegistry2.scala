package io.amient.affinity.avro.refactor

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.ConfluentSchemaRegistry.CfAvroConf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.avro.util.ConfluentSchemaRegistryClient
import org.apache.avro.Schema

class ConfluentSchemaRegistry2(config: Config) extends AvroSchemaRegistry2 {
  val merged = config.withFallback(ConfigFactory.defaultReference.getConfig(AvroSerde.AbsConf.Avro.path))
  val conf = new CfAvroConf().apply(merged)
  val client = new ConfluentSchemaRegistryClient(conf.ConfluentSchemaRegistryUrl())

  override def close(): Unit = ()

  override protected def loadSchema(id: Int): Schema = client.getSchema(id)

  override protected def registerSchema(subject: String, schema: Schema): Int = client.registerSchema(subject, schema)

}

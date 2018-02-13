package io.amient.affinity.avro.refactor

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.MemorySchemaRegistry
import org.apache.avro.{Schema, SchemaValidatorBuilder}

class MemorySchemaRegistry2(config: Config) extends AvroSchemaRegistry2 {

  def this() = this(ConfigFactory.empty)

  val conf = MemorySchemaRegistry.Conf(config)

  val universe = if (conf.ID.isDefined) {
    MemorySchemaRegistry.createUniverse(Some(conf.ID()))
  } else {
    MemorySchemaRegistry.createUniverse()
  }

  private val validator = new SchemaValidatorBuilder().canReadStrategy().validateLatest()

  /**
    * @param id
    * @return schema
    */
  override protected def loadSchema(id: Int): Schema = universe.schemas.get(id)

  /**
    *
    * @param subject
    * @param schema
    * @return
    */
  override protected def registerSchema(subject: String, schema: Schema): Int = {
    val id = universe.getOrRegister(schema)
    universe.updateSubject(subject, id)
    id
  }

  override def close() = universe.close()
}

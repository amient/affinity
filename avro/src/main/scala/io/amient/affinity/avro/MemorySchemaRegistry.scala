package io.amient.affinity.avro

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.MemorySchemaRegistry.MemorySchemaRegistryConf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.config.CfgStruct
import org.apache.avro.{Schema, SchemaValidator}

import scala.collection.JavaConversions._

object MemorySchemaRegistry {

  object Conf extends MemorySchemaRegistryConf {
    override def apply(config: Config): MemorySchemaRegistryConf = new MemorySchemaRegistryConf().apply(config)
  }

  class MemorySchemaRegistryConf extends CfgStruct[MemorySchemaRegistryConf](classOf[AvroConf]) {
    val ID = integer("schema.registry.id", false)
  }

  val multiverse = new ConcurrentHashMap[Int, Universe]()

  def createUniverse(reuse: Option[Int] = None): Universe = synchronized {
    reuse match {
      case Some(id) if multiverse.containsKey(id) => multiverse(id)
      case Some(id) =>
        val universe = new Universe(id)
        multiverse += id -> universe
        universe
      case None =>
        val id = (if (multiverse.isEmpty) 1 else multiverse.keys.max + 1)
        val universe = new Universe(id)
        multiverse += id -> universe
        universe
    }
  }

  class Universe(val id: Int) {
    val schemas = new ConcurrentHashMap[Int, Schema]()
    val subjects = new ConcurrentHashMap[String, List[Int]]()

    def getOrRegister(schema: Schema): Int = synchronized {
      schemas.find(_._2 == schema) match {
        case None =>
          val newId = schemas.size
          schemas.put(newId, schema)
          newId
        case Some((id, _)) => id
      }
    }

    def updateSubject(subject: String, schemaId: Int, validator: SchemaValidator): Unit = synchronized {
      val existing = Option(subjects.get(subject)).getOrElse(List())
      validator.validate(schemas(schemaId), existing.map(id => schemas(id)))
      if (!existing.contains(schemaId)) {
        subjects.put(subject, (existing :+ schemaId))
      }
    }
  }

}

class MemorySchemaRegistry(universe: MemorySchemaRegistry.Universe) extends AvroSerde with AvroSchemaRegistry {

  def this(conf: MemorySchemaRegistryConf) = this {
    MemorySchemaRegistry.createUniverse(if (conf.ID.isDefined) Some(conf.ID()) else None)
  }

  def this(config: Config) = this(MemorySchemaRegistry.Conf(config))

  def this() = this(ConfigFactory.empty)

  /**
    * @param id
    * @return schema
    */
  override protected def loadSchema(id: Int): Schema = {
    universe.schemas.get(id) match {
      case null => throw new Exception(s"Schema $id is not registered in universe ${universe.id}")
      case schema => schema
    }
  }

  /**
    *
    * @param subject
    * @param schema
    * @return
    */
  override protected def registerSchema(subject: String, schema: Schema): Int = {
    val id = universe.getOrRegister(schema)
    universe.updateSubject(subject, id, validator)
    id
  }

  override def close() = ()
}

package io.amient.affinity.avro

import java.nio.file.Files

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.LocalSchemaRegistry.LocalAvroConf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import org.apache.avro.Schema

import scala.collection.JavaConverters._
import scala.io.Source


object LocalSchemaRegistry {

  object LocalAvroConf extends LocalAvroConf {
    override def apply(config: Config) = new LocalAvroConf().apply(config)
  }
  class LocalAvroConf extends CfgStruct[LocalAvroConf](classOf[AvroConf]) {
    val DataPath = filepath("schema.registry.path", true)
  }

}


class LocalSchemaRegistry(config: Config) extends AvroSerde with AvroSchemaRegistry {

  val merged = config.withFallback(ConfigFactory.defaultReference.getConfig(AvroSerde.AbsConf.Avro.path))
  val conf = new LocalAvroConf().apply(merged)
  val dataPath = conf.DataPath()

  if (!Files.exists(dataPath)) Files.createDirectories(dataPath)

  override def close() = ()

  /**
    * @param id
    * @return schema
    */
  override protected def loadSchema(id: Int): Schema = {
    new Schema.Parser().parse(dataPath.resolve(s"$id.avsc").toFile)
  }

  /**
    *
    * @param subject
    * @param schema
    * @return
    */
  override protected def registerSchema(subject: String, schema: Schema): Int = hypersynchronized {
    val s = dataPath.resolve(s"$subject.dat")
    val versions: Map[Schema, Int] = if (Files.exists(s)) {
      Source.fromFile(s.toFile).mkString.split(",").toList.map(_.toInt).map {
        case id => getSchema(id) -> id
      }.toMap
    } else {
      Map.empty
    }
    versions.get(schema).getOrElse {
      validator.validate(schema, versions.map(_._1).asJava)
      val id = (0 until Int.MaxValue).find(i => !Files.exists(dataPath.resolve(s"$i.avsc"))).max
      val schemaPath = dataPath.resolve(s"$id.avsc")
      Files.createFile(schemaPath)
      Files.write(schemaPath, schema.toString(true).getBytes("UTF-8"))
      id
    }
  }

  private def hypersynchronized[X](func: => X) = synchronized {
    val file = dataPath.resolve(".lock").toFile

    def getLock(countDown: Int = 30): Unit = {
      if (!file.createNewFile()) if (countDown > 0) {
        Thread.sleep(1000)
        getLock(countDown - 1)
      } else throw new java.nio.file.FileAlreadyExistsException("atomic createNewFile failed")
    }

    getLock()
    try {
      func
    } finally {
      file.delete()
    }
  }

}

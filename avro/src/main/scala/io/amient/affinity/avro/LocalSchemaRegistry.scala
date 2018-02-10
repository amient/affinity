/*
 * Copyright 2016 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.affinity.avro

import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.LocalSchemaRegistry.LocalAvroConf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import org.apache.avro.{Schema, SchemaValidationException, SchemaValidatorBuilder}

import scala.collection.JavaConversions._

object LocalSchemaRegistry {

  object Conf extends Conf {
    override def apply(config: Config): Conf = new Conf().apply(config)
  }

  class Conf extends CfgStruct[Conf](Cfg.Options.IGNORE_UNKNOWN) {
    val Avro = struct("affinity.avro", new LocalAvroConf, false)
  }

  class LocalAvroConf extends CfgStruct[LocalAvroConf](classOf[AvroConf]) {
    val DataPath = filepath("schema.registry.path", true)
  }

}

class LocalSchemaRegistry(config: Config) extends AvroSerde {
  val merged = config.withFallback(ConfigFactory.defaultReference.getConfig(AvroSerde.AbsConf.Avro.path))
  val conf = new LocalAvroConf().apply(merged)
  val dataPath = conf.DataPath()

  private val validator = new SchemaValidatorBuilder().mutualReadStrategy().validateLatest()

  if (!Files.exists(dataPath)) Files.createDirectories(dataPath)

  override def close(): Unit = ()

  override private[avro] def registerSchema(subject: String, schema: Schema, existing: List[Schema]): Int = synchronized {
    try {
      validator.validate(schema, existing)
    } catch {
      case e: SchemaValidationException =>
        throw new RuntimeException(s"subject: $subject, schema: ${schema.getFullName} validation error", e)
    }
    val id = (0 until Int.MaxValue).find(i => !Files.exists(dataPath.resolve(s"$i.avsc"))).max
    val schemaPath = dataPath.resolve(s"$id.avsc")
    Files.createFile(schemaPath)
    Files.write(schemaPath, schema.toString(true).getBytes("UTF-8"))
    id
  }

  override private[avro] def hypersynchronized[X](f: => X) = synchronized {
    val file = dataPath.resolve(".lock").toFile

    def getLock(countDown: Int = 30): Unit = {
      if (!file.createNewFile()) if (countDown > 0) {
        Thread.sleep(1000)
        getLock(countDown - 1)
      } else throw new java.nio.file.FileAlreadyExistsException("atomic createNewFile failed")
    }

    getLock()
    try {
      f
    } finally {
      file.delete()
    }
  }


  override private[avro] def getAllRegistered: List[(Int, String, Schema)] = {
    val builder = List.newBuilder[(Int, String, Schema)]
    Files.walkFileTree(dataPath, new SimpleFileVisitor[Path]() {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        val filename = file.getFileName.toString
        if (filename.matches("^[0-9]+\\.avsc$")) {
          val id = filename.split("\\.")(0).toInt
          val schema = new Schema.Parser().parse(file.toFile)
          builder += ((id, schema.getFullName, schema))
        }
        super.visitFile(file, attrs)
      }
    })
    builder.result()
  }
}

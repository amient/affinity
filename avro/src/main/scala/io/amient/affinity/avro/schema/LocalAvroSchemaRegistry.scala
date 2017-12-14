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

package io.amient.affinity.avro.schema

import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import com.typesafe.config.Config
import io.amient.affinity.avro.AvroSerde
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import org.apache.avro.{Schema, SchemaValidatorBuilder}

import scala.collection.JavaConversions._

object LocalAvroSchemaRegistry {
  class Conf extends CfgStruct[Conf](Cfg.Options.IGNORE_UNKNOWN) {
    val LocalAvro = struct("affinity.avro.local-schema-registry", new LocalAvroConf, false)
  }

  class LocalAvroConf extends CfgStruct[LocalAvroConf] {
    val DataPath = filepath("data.path", true)
  }

}

class LocalAvroSchemaRegistry(config: Config) extends AvroSerde {

  val conf = new LocalAvroSchemaRegistry.Conf()(config).LocalAvro

  private val validator = new SchemaValidatorBuilder().mutualReadStrategy().validateLatest()

  if (!Files.exists(conf.DataPath())) Files.createDirectories(conf.DataPath())

  override def close(): Unit = ()

  override private[schema] def registerSchema(subject: String, schema: Schema, existing: List[Schema]): Int = synchronized {
    validator.validate(schema, existing)
    val id = (0 until Int.MaxValue).find(i => !Files.exists(conf.DataPath().resolve(s"$i.avsc"))).max
    val schemaPath = conf.DataPath().resolve(s"$id.avsc")
    Files.createFile(schemaPath)
    Files.write(schemaPath, schema.toString(true).getBytes("UTF-8"))
    id
  }

  override private[schema] def hypersynchronized[X](f: => X) = synchronized {
    val file = conf.DataPath().resolve(".lock").toFile
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


  override private[schema] def getAllRegistered: List[(Int, String, Schema)] = {
    val builder = List.newBuilder[(Int, String, Schema)]
    Files.walkFileTree(conf.DataPath(), new SimpleFileVisitor[Path]() {
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

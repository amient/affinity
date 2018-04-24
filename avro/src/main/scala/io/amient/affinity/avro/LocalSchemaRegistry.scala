/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
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

import java.nio.file.{Files, Path}

import com.typesafe.config.Config
import io.amient.affinity.avro.LocalSchemaRegistry.LocalAvroConf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.config.CfgStruct
import org.apache.avro.Schema

import scala.collection.JavaConverters._
import scala.io.Source


object LocalSchemaRegistry {

  object LocalAvroConf extends LocalAvroConf {
    override def apply(config: Config) = new LocalAvroConf().apply(config)
  }

  class LocalAvroConf extends CfgStruct[LocalAvroConf](classOf[AvroConf]) {
    val DataPath = filepath("schema.registry.path", true).doc("local file path under which schemas will be stored")
  }

}


class LocalSchemaRegistry(dataPath: Path) extends AvroSerde with AvroSchemaRegistry {

  def this(config: Config) = this(LocalAvroConf(config).DataPath())

  def checkDataPath(): Unit = {
    require(dataPath != null)
    if (!Files.exists(dataPath)) Files.createDirectories(dataPath)
  }

  override def close() = ()

  /**
    * @param id
    * @return schema
    */
  override protected def loadSchema(id: Int): Schema = {
    checkDataPath()
    new Schema.Parser().parse(dataPath.resolve(s"$id.avsc").toFile)
  }

  /**
    *
    * @param subject
    * @param schema
    * @return
    */
  override protected def registerSchema(subject: String, schema: Schema): Int = hypersynchronized {
    checkDataPath()
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
      val updatedVersions = versions + (schema -> id)
      Files.write(s, updatedVersions.values.mkString(",").getBytes("UTF-8"))
      id
    }
  }

  private def hypersynchronized[X](func: => X) = synchronized {

    checkDataPath()
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

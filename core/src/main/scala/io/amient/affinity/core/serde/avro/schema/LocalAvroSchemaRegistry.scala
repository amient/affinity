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

package io.amient.affinity.core.serde.avro.schema

import java.io.{FileInputStream, RandomAccessFile}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import com.typesafe.config.Config
import io.amient.affinity.core.serde.avro.AvroSerde
import org.apache.avro.Schema

import scala.collection.JavaConverters._

object LocalAvroSchemaRegistry {
  final val CONFIG_LOCAL_SCHEMA_PROVIDER_DATA_PATH = "affinity.avro.local-schema-registry.data.path"
}

class LocalAvroSchemaRegistry(config: Config) extends AvroSerde with EmbeddedAvroSchemaProvider {

  import LocalAvroSchemaRegistry._

  val dataPath = Paths.get(config.getString(CONFIG_LOCAL_SCHEMA_PROVIDER_DATA_PATH))
  if (!Files.exists(dataPath)) Files.createDirectories(dataPath)
  else {
    Files.walkFileTree(dataPath, new SimpleFileVisitor[Path]() {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        val filename = file.getFileName.toString
        if (filename.matches("^[0-9]+\\.avsc$")) {
          val id = filename.split("\\.")(0).toInt
          val schema = new Schema.Parser().parse(file.toFile)
          try {
            internal.put(id, (Class.forName(schema.getFullName), schema))
          } catch {
            case _: ClassNotFoundException =>
              //s"schema $id for ${schema.getFullName} no longer has a compile time class associated"
              internal.put(id, (null, schema))
          }
        }
        super.visitFile(file, attrs)
      }
    })
  }

  override def close(): Unit = ()

  override private[schema] def registerSchema(cls: Class[_], schema: Schema): Int = synchronized {
    val isNew = internal.asScala.filter(_._2 == (cls, schema)).isEmpty
    val id = super.registerSchema(cls, schema)
    if (isNew) {
      val schemaPath = dataPath.resolve(s"$id.avsc")
      Files.createFile(schemaPath)
      Files.write(schemaPath, schema.toString(true).getBytes("UTF-8"))
    }
    id
  }

  override private[schema] def hypersynchronized[X](f: => X) = synchronized {
    val in = new RandomAccessFile(dataPath.resolve(".lock").toFile, "rw")
    try {
      val lock = in.getChannel.lock()
      try {
        f
      } finally {
        lock.release()
      }
    } finally {
      in.close()
    }

  }


}

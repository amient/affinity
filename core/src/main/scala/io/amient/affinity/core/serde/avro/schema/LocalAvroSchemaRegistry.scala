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

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._

import akka.actor.ExtendedActorSystem
import io.amient.affinity.core.serde.avro.AvroSerde
import org.apache.avro.Schema

import scala.collection.JavaConverters._

object LocalAvroSchemaRegistry {
  final val CONFIG_LOCAL_SCHEMA_PROVIDER_DATA_PATH = "affinity.local-schema-registry.data.path"
}

class LocalAvroSchemaRegistry(system: ExtendedActorSystem) extends AvroSerde with EmbeddedAvroSchemaProvider {

  import LocalAvroSchemaRegistry._

  val dataPath = Paths.get(system.settings.config.getString(CONFIG_LOCAL_SCHEMA_PROVIDER_DATA_PATH))
  if (!Files.exists(dataPath)) Files.createDirectories(dataPath)
  else {
    Files.walkFileTree(dataPath, new SimpleFileVisitor[Path]() {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        val id = file.getFileName.toString.split("\\.")(0).toInt
        val schema = new Schema.Parser().parse(file.toFile)
        try {
          val cls = Class.forName(schema.getFullName)
          internal.put(id, (cls, schema))
        } catch {
          case removed: ClassNotFoundException =>
            internal.put(id, (null, schema))
            system.log.warning(s"schema $id for ${schema.getFullName} no longer has a compile time class associated")
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

}

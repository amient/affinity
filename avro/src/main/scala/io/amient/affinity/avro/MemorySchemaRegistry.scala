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

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.MemorySchemaRegistry.MemAvroConf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.config.CfgStruct
import org.apache.avro.{Schema, SchemaValidator}

import scala.collection.JavaConverters._

object MemorySchemaRegistry {

  object MemAvroConf extends MemAvroConf {
    override def apply(config: Config) = new MemAvroConf().apply(config)
  }
  class MemAvroConf extends CfgStruct[MemAvroConf](classOf[AvroConf]) {
    val ID = integer("schema.registry.id", false)
  }

  val multiverse = new ConcurrentHashMap[Int, Universe]()

  def createUniverse(reuse: Option[Int] = None): Universe = synchronized {
    reuse match {
      case Some(id) if multiverse.containsKey(id) => multiverse.get(id)
      case Some(id) =>
        val universe = new Universe(id)
        multiverse.asScala += id -> universe
        universe
      case None =>
        val id = (if (multiverse.isEmpty) 1 else multiverse.asScala.keys.max + 1)
        val universe = new Universe(id)
        multiverse.asScala += id -> universe
        universe
    }
  }

  class Universe(val id: Int) {
    val schemas = new ConcurrentHashMap[Int, Schema]()
    val subjects = new ConcurrentHashMap[String, List[Int]]()

    def getOrRegister(schema: Schema): Int = synchronized {
      schemas.asScala.find(_._2 == schema) match {
        case None =>
          val newId = schemas.size
          schemas.put(newId, schema)
          newId
        case Some((id, _)) => id
      }
    }

    def updateSubject(subject: String, schemaId: Int, validator: SchemaValidator): Unit = synchronized {
      val existing = Option(subjects.get(subject)).getOrElse(List())
      validator.validate(schemas.get(schemaId), existing.map(id => schemas.get(id)).asJava)
      if (!existing.contains(schemaId)) {
        subjects.put(subject, (existing :+ schemaId))
      }
    }
  }

}

class MemorySchemaRegistry(universe: MemorySchemaRegistry.Universe) extends AvroSerde with AvroSchemaRegistry {

  def this(conf: MemAvroConf) = this {
    MemorySchemaRegistry.createUniverse(if (conf.ID.isDefined) Some(conf.ID()) else None)
  }

  def this(config: Config) = this(MemAvroConf(AvroSerde.AbsConf).apply(config))

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

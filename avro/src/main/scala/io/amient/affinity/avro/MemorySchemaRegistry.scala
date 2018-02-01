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

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.record.AvroSerde
import org.apache.avro.{Schema, SchemaValidatorBuilder}

import scala.collection.JavaConversions._
import scala.collection.immutable.Seq
import scala.collection.mutable


object MemorySchemaRegistry {

  final val ID = "schema.registry.id"

  val multiverse = new mutable.HashMap[Int, Universe]()

  def createUniverse(reuse: Option[Int] = None): Universe = reuse match {
    case Some(id) if multiverse.contains(id) => multiverse(id)
    case Some(id) =>
      val universe = new Universe
      multiverse += id -> universe
      universe
    case None =>
      val universe = new Universe
      multiverse += (if (multiverse.isEmpty) 1 else multiverse.keys.max + 1) -> universe
      universe
  }

  class Universe {
    val schemas = new ConcurrentHashMap[Int, Schema]()
    val subjects = new ConcurrentHashMap[String, List[Int]]()
    def getOrRegister(schema: Schema): Int = {
      schemas.find(_._2 == schema) match {
        case None =>
          val newId = schemas.size
          schemas.put(newId, schema)
          newId
        case Some((id, _)) => id
      }
    }
    def updateSubject(subject: String, schemaId: Int): Unit = {
      subjects.put(subject, (Option(subjects.get(subject)).getOrElse(List()) :+ schemaId))
    }
  }
}

class MemorySchemaRegistry(config: Config) extends AvroSerde with AvroSchemaRegistry {

  def this() = this(ConfigFactory.empty)

  import MemorySchemaRegistry._

  val universe = if (config.hasPath(ID)) createUniverse(Some(config.getInt(ID))) else createUniverse()

  private val validator = new SchemaValidatorBuilder().canReadStrategy().validateLatest()

  override private[avro] def registerSchema(subject: String, schema: Schema, existing: List[Schema]): Int = synchronized {
    validator.validate(schema, existing)
    val schemaId: Int = universe.getOrRegister(schema)
    universe.updateSubject(subject, schemaId)
    schemaId
  }

  override private[avro] def getAllRegistered: List[(Int, String, Schema)] = {
    universe.subjects.flatMap {
      case (subject: String, ids: Seq[Int]) => ids.map {
        case id => (id, subject, universe.schemas.get(id))
      }
    }.toList
  }

  override private[avro] def hypersynchronized[X](f: => X): X = synchronized(f)

}

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

import io.amient.affinity.core.serde.avro.AvroRecord
import org.apache.avro.Schema

import scala.collection.{immutable, mutable}
import scala.reflect.runtime.universe._

/**
  * This trait ensures that all the getters are fast and thread-safe.
  * Implementing classes do not have to be thread-safe
  */
trait AvroSchemaProvider {

  private val register = mutable.HashSet[(Int, Schema, Class[_], Type)]()

  @volatile private var cache1: immutable.Map[Int, (Type, Schema)] = Map()

  @volatile private var cache2: immutable.Map[Schema, Int] = Map()

  @volatile private var cache3 = immutable.Map[Int, Option[(Type, Schema)]]()

  /**
    * Get schema id which is associated with a concrete schema instance
    * @param schema
    * @return
    */
  final def schema(schema: Schema): Option[Int] = cache2.get(schema)

  /**
    * Get Type and Schema by a schema Id.
    * Newly registered type versions by other instances may not be associated
    * with any runtime type in this instance but their schemas will be available in
    * the shared external schema registry.
    *
    * @param id schema id
    * @return
    */
  final def schema(id: Int): Option[(Type, Schema)] = {
    cache1.get(id) match {
      case Some(existing) => Some(existing)
      case None =>
        cache3.get(id) match {
          case Some(cached) => cached
          case None => synchronized {
            val result = getSchema(id).map { schema =>
              //lookup type in the current registry, if the compile-time type is new, then null
              val sameFQNs = cache1.values.filter(_._2.getFullName == schema.getFullName).map(_._1)
              val tpe = sameFQNs.headOption.getOrElse(null)
              (tpe, schema)
            }
            cache3 = cache3 + (id -> result)
            result
          }
        }

    }
  }

  /**
    * register compile-time type with its current schema
    *
    * @param cls compile time class
    * @tparam T compile time type
    * @return unique schema id
    */
  final def register[T: TypeTag](cls: Class[T]): Int = synchronized {
    register(typeOf[T], cls, AvroRecord.inferSchema(cls))
  }

  /**
    * register compile-time type with older schema version
    *
    * @param schema schema to register with the compile time class/type
    * @param cls compile time class
    * @tparam T compile time type
    * @return unique schema id
    */
  final def register[T: TypeTag](cls: Class[T], schema: Schema): Int = synchronized {
    register(typeOf[T], cls, schema)
  }

  private def register(tpe: Type, cls: Class[_], schema: Schema): Int = synchronized {

    val alreadyRegisteredId: Int = (getVersions(cls).map { case (id2, schema2) =>
      register += ((id2, schema2, cls, tpe))
      if (schema2 == schema) id2 else -1
    } :+ (-1)) .max

    val id = if (alreadyRegisteredId == -1) {
      val newlyRegisteredId = registerSchema(cls, schema)
      register += ((newlyRegisteredId, schema, cls, tpe))
      newlyRegisteredId
    } else {
      alreadyRegisteredId
    }

    cache1 = register.map { case (id2, schema2, cls2, tpe2) =>
      val tpe3 = if (cls == cls2) tpe else tpe2 //update all schema versions for the same class with its runtime type
      id2 -> (tpe3, schema2)
    }.toMap

    cache2 = register.map { case (id2, schema2, cls2, tpe2) => schema2 -> id2 }.toMap

    id
  }

  private[schema] def registerSchema(cls: Class[_], schema: Schema): Int

  private[schema] def getSchema(id: Int): Option[Schema]

  private[schema] def getVersions(cls: Class[_]): List[(Int, Schema)]



}

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

import scala.collection.immutable
import scala.reflect.runtime.universe._

trait AvroSchemaProvider {

  @volatile private var reg1: immutable.Map[Class[_], Int] = Map()

  @volatile private var reg2: immutable.Map[Int, (Type, Schema)] = Map()

  @volatile private var reg3: immutable.Map[Schema, Int] = Map()

  private var forwardCache = immutable.Map[Int, Option[(Type, Schema)]]()

  final def schema(cls: Class[_]): Option[Int] = reg1.get(cls)

  final def schema(schema: Schema): Option[Int] = reg3.get(schema)

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
    reg2.get(id) match {
      case Some(existing) => Some(existing)
      case None =>
        forwardCache.get(id) match {
          case Some(cached) => cached
          case None => synchronized {
            val result = getSchema(id).map { schema =>
              //lookup type in the current registry, if the compile-time type is new, then null
              val sameFQNs = reg2.values.filter(_._2.getFullName == schema.getFullName).map(_._1)
              val tpe = sameFQNs.headOption.getOrElse(null)
              (tpe, schema)
            }
            forwardCache = forwardCache + (id -> result)
            println(s"!new $id $result")
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

  private final def register(tpe: Type, cls: Class[_], schema: Schema): Int = synchronized {
    val id = registerType(tpe, cls, schema)
    val register = getAllSchemas
    reg1 = register.map { case (id2, schema2, cls2, tpe2) => cls2 -> id2 }.toMap
    reg2 = register.map { case (id2, schema2, cls2, tpe2) =>
      val tpe3 = if (cls == cls2) tpe else tpe2 //update all schema versions for the same class with its runtime type
      id2 -> (tpe3, schema2)
    }.toMap
    reg3 = register.map { case (id2, schema2, cls2, tpe2) => schema2 -> id2 }.toMap
    id
  }

  protected def getSchema(id: Int): Option[Schema]

  protected def registerType(tpe: Type, cls: Class[_], schema: Schema): Int

  protected def getAllSchemas: immutable.List[(Int, Schema, Class[_], Type)]

}

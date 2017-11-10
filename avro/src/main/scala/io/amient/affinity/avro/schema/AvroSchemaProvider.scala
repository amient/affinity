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

import io.amient.affinity.avro.AvroRecord
import org.apache.avro.Schema

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}
import scala.reflect.runtime.universe._

/**
  * This trait ensures that all the getters are fast and thread-safe.
  * Implementing classes do not have to be thread-safe
  */
trait AvroSchemaProvider {

  private var cache1: immutable.Map[Int, (Type, Schema)] = Map()

  private var cache3 = immutable.Map[Int, Option[(Type, Schema)]]()

  private var cache4 = immutable.Map[String, Int]()

  private[schema] def registerSchema(cls: Class[_], schema: Schema): Int

  private[schema] def getAllRegistered: List[(Int, Schema)]

  private[schema] def hypersynchronized[X](f: => X): X

  private val registration = ListBuffer[(Type, Class[_], Schema)]()

  register(classOf[Null])
  register(classOf[Boolean])
  register(classOf[Int])
  register(classOf[Long])
  register(classOf[Float])
  register(classOf[Double])
  register(classOf[String])

  def describeSchemas: Map[Int, (Type, Schema)] = cache1

  /**
    * Get current current schema for the compile time class
    *
    * @param fqn fully qualified name of the schema
    * @return
    */
  final def schema(fqn: String): Option[Int] = cache4.get(fqn)

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
          case None => throw new NoSuchElementException(s"Schema with id $id is not recognized")
        }
    }
  }

  final def initialize(): List[Int] = hypersynchronized {
    val all: List[(Int, Schema)] = getAllRegistered
    def getVersions(cls: Class[_]): List[(Int, Schema)] = {
      all.filter(_._2.getFullName match {
        case "null" => cls == classOf[Null]
        case "boolean" => cls == classOf[Boolean]
        case "int" => cls == classOf[Int]
        case "long" => cls == classOf[Long]
        case "float" => cls == classOf[Float]
        case "double" => cls == classOf[Double]
        case "string" => cls == classOf[String]
        case "bytes" => cls == classOf[java.nio.ByteBuffer]
        case fqn => cls.getName == fqn
      })
    }
    val result = ListBuffer[Int]()
    val _register = mutable.HashSet[(Int, Schema, Class[_], Type)]()
    registration.result.foreach {
      case (tpe, cls, schema) =>
        val alreadyRegisteredId: Int = (getVersions(cls).map { case (id2, schema2) =>
          _register  += ((id2, schema2, cls, tpe))
          if (schema2 == schema) id2 else -1
        } :+ (-1)).max

        val schemaId = if (alreadyRegisteredId == -1) {
          val newlyRegisteredId = registerSchema(cls, schema)
          _register  += ((newlyRegisteredId, schema, cls, tpe))
          newlyRegisteredId
        } else {
          alreadyRegisteredId
        }

        result += schemaId

        cache1 = _register .map { case (id2, schema2, cls2, tpe2) =>
          val tpe3 = if (cls == cls2) tpe else tpe2 //update all schema versions for the same class with its runtime type
          id2 -> (tpe3, schema2)
        }.toMap

        cache4 += schema.getFullName -> schemaId
    }

    result.result()
  }

  /**
    * register compile-time type with its current schema
    *
    * @param cls compile time class
    * @tparam T compile time type
    * @return unique schema id
    */
  final def register[T: TypeTag](cls: Class[T]): Unit = {
    registration += ((typeOf[T], cls, AvroRecord.inferSchema(cls)))
  }

  /**
    * register compile-time type with older schema version
    *
    * @param schema schema to register with the compile time class/type
    * @param cls    compile time class
    * @tparam T compile time type
    * @return unique schema id
    */
  final def register[T: TypeTag](cls: Class[T], schema: Schema): Unit = {
    registration += ((typeOf[T], cls, schema))
  }



}

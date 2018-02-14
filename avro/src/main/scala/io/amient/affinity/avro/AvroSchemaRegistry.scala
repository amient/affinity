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

import java.io.Closeable
import java.util.function.Supplier

import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.core.util.ThreadLocalCache
import org.apache.avro.{Schema, SchemaValidatorBuilder}

import scala.reflect.runtime.universe._

/**
  * This trait ensures that all the getters are fast and thread-safe.
  * Implementing classes do not have to be thread-safe
  */
trait AvroSchemaRegistry extends Closeable {

  val validator = new SchemaValidatorBuilder().mutualReadStrategy().validateAll() //equivalent to FULL_TRANSITIVE

  register[Null]("null")
  register[Boolean]("boolean")
  register[Int]("int")
  register[Long]("long")
  register[Float]("float")
  register[Double]("double")
  register[String]("string")
  register[Array[Byte]]("bytes")

  //register compile-time type with its current/actual schema and its fqn as subject
  final def register[T: TypeTag]: Int = register[T](typeOf[T].typeSymbol.fullName)

  //register compile-time type with a specific schema version and its fqn as subject
  final def register[T: TypeTag](schema: Schema): Int = register(typeOf[T].typeSymbol.fullName, schema)

  //register compile-time type with its current/actual schema and a custom subject
  final def register[T: TypeTag](subject: String): Int = register(subject, AvroRecord.inferSchema[T])

  final def register(cls: Class[_]): Int = register(cls, cls.getName)

  final def register(cls: Class[_], subject: String): Int = register(subject, AvroRecord.inferSchema(cls))

  final def register(subject: String, schema: Schema): Int = Versions.getOrInitialize(subject, schema)

  final def from(data: Any): (Int, Schema) = {
    val schema = AvroRecord.inferSchema(data)
    val schemaId = register(schema.getFullName, schema)
    schemaId -> schema
  }

  final def from(data: Any, subject: String): (Int, Schema) = {
    val schema = AvroRecord.inferSchema(data)
    val schemaId = register(subject, schema)
    schemaId -> schema
  }


  /**
    * Get current current runtime schema version for a given schema
    *
    * @param other schema version
    * @return
    */
  final def getRuntimeSchema(other: Schema): (Int, Schema) = getRuntimeSchema(fqn = other.getFullName)

  /**
    *
    * @param fqn
    * @return
    */
  final def getRuntimeSchema(fqn: String): (Int, Schema) = FQNs.getOrInitialize(fqn)


  /**
    * Get Type and Schema by a schema Id.
    * Newly registered type versions by other instances may not be associated
    * with any runtime type in this instance but their schemas will be available in
    * the shared external schema registry.
    *
    * @param id schema id
    * @return
    */
  final def getSchema(id: Int): Schema = Schemas.getOrInitialize(id)

  /**
    * @param id
    * @return schema
    */
  protected def loadSchema(id: Int): Schema

  /**
    *
    * @param subject
    * @param schema
    * @return
    */
  protected def registerSchema(subject: String, schema: Schema): Int

  private object Schemas extends ThreadLocalCache[Int, Schema] {
    def getOrInitialize(id: Int): Schema = getOrInitialize(id, new Supplier[Schema] {
      override def get(): Schema = loadSchema(id)
    })
  }

  private object Versions extends ThreadLocalCache[(String, Schema), Int] {
    def getOrInitialize(subject: String, schema: Schema): Int = {
      getOrInitialize((subject, schema), new Supplier[Int] {
        override def get(): Int = {
          val sid = registerSchema(subject, schema)
          Schemas.initialize(sid, schema)
          sid
        }
      })
    }
  }

  private object FQNs extends ThreadLocalCache[String, (Int, Schema)] {
    def getOrInitialize(fqn: String): (Int, Schema) = {
      getOrInitialize(fqn, new Supplier[(Int, Schema)] {
        override def get(): (Int, Schema) = {
          val schema = AvroRecord.inferSchema(fqn)
          (Versions.getOrInitialize(schema.getFullName, schema), schema)
        }
      })
    }
  }


}

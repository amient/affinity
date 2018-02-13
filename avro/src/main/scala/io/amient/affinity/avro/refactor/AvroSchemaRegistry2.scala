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

package io.amient.affinity.avro.refactor

import java.io.Closeable

import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.avro.util.ThreadLocalCache
import org.apache.avro.Schema

import scala.reflect.runtime.universe._

/**
  * This trait ensures that all the getters are fast and thread-safe.
  * Implementing classes do not have to be thread-safe
  */
trait AvroSchemaRegistry2 extends Closeable {

  of[Null]("null")
  of[Boolean]("boolean")
  of[Int]("int")
  of[Long]("long")
  of[Float]("float")
  of[Double]("double")
  of[String]("string")
  of[Array[Byte]]("bytes")

  //register compile-time type with its current/actual schema and its fqn as subject
  final def of[T: TypeTag]: Int = of[T](typeOf[T].typeSymbol.fullName)

  //register compile-time type with a specific schema version and its fqn as subject
  final def of[T: TypeTag](schema: Schema): Int = of(typeOf[T].typeSymbol.fullName, schema)

  //register compile-time type with its current/actual schema and a custom subject
  final def of[T: TypeTag](subject: String): Int = of(subject, AvroRecord.inferSchema(typeOf[T]))

  final def of(cls: Class[_]): Int = of(cls, cls.getName)

  final def of(cls: Class[_], subject: String): Int = of(subject, AvroRecord.inferSchema(cls))

  final def of(subject: String, schema: Schema): Int = Versions.getOrInitialize(subject, schema)

//  final def get(data: Any): (Int, Schema) = {
//    val schema = AvroRecord.inferSchema(data)
//    val schemaId = of(schema.getFullName, schema)
//    schemaId -> schema
//  }
//
//  final def get(data: Any, subject: String): (Int, Schema) = {
//    val schema = AvroRecord.inferSchema(data)
//    val schemaId = of(subject, schema)
//    schemaId -> schema
//  }


  /**
    * Get current current runtime schema version for a given schema
    *
    * @param other schema version
    * @return
    */
  final def getRuntimeSchema(other: Schema): (Int, Schema) = FQNs.getOrInitialize(fqn = other.getFullName)


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
    def getOrInitialize(id: Int): Schema = getOrInitialize(id, loadSchema(id))
  }

  private object Versions extends ThreadLocalCache[(String, Schema), Int] {
    def getOrInitialize(subject: String, schema: Schema): Int = getOrInitialize((subject, schema), {
      registerSchema(subject, schema)
    })
  }

  private object FQNs extends ThreadLocalCache[String, (Int, Schema)] {
    def getOrInitialize(fqn: String): (Int, Schema) = getOrInitialize(fqn, {
      val schema = AvroRecord.inferSchema(fqn)
      (Versions.getOrInitialize(schema.getFullName, schema), schema)
    })
  }



}

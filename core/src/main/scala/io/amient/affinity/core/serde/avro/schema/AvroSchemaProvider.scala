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

// TODO #9 This trait is not fully Thread-Safe at the moment but once the API is completed it should be reimplemented.
trait AvroSchemaProvider {

  private var reg1: immutable.Map[Class[_], Int] = Map()

  private var reg2: immutable.Map[Int, (Type, Class[_], Schema)] = Map()

  private var reg3: immutable.Map[Schema, Int] = Map()

  final def schema(id: Int): (Type, Class[_], Schema) = reg2(id)

  final def schema(cls: Class[_]): Option[Int] = reg1.get(cls)

  final def schema(schema: Schema): Option[Int] = reg3.get(schema)

  /**
    * register run-time type and its new schema
    *
    * @param schema
    * @param className
    * @return unique schema id
    */
  final def register(className: String, schema: Schema): Int = synchronized {
    val (tpe, cls, currentSchema) = reg2(reg1(Class.forName(className)))
    register(tpe, cls, schema)
  }

  /**
    * register compile-time type with its current schema
    *
    * @param cls
    * @tparam T
    * @return unique schema id
    */
  final def register[T: TypeTag](cls: Class[T]): Int = synchronized {
    register(typeOf[T], cls, AvroRecord.inferSchema(cls))
  }

  /**
    * register compile-time type with older schema version
    *
    * @param schema
    * @param cls
    * @tparam T
    * @return unique schema id
    */
  final def register[T: TypeTag](cls: Class[T], schema: Schema): Int = synchronized {
    register(typeOf[T], cls, schema)
  }

  private final def register(tpe: Type, cls: Class[_], schema: Schema): Int = synchronized {
    val id = registerType(tpe, cls, schema)
    val register = getAllSchemas()
    reg1 = register.map { case (id, schema, cls, tpe) => cls -> id }.toMap
    reg2 = register.map { case (id, schema, cls, tpe) => id -> (tpe, cls, schema) }.toMap
    reg3 = register.map { case (id, schema, cls, tpe) => schema -> id }.toMap
    id
  }

  /**
    * @param tpe
    * @param cls
    * @param schema
    * @return unique schema id
    */
  protected def registerType(tpe: Type, cls: Class[_], schema: Schema): Int

  protected def getAllSchemas(): immutable.List[(Int, Schema, Class[_], Type)]

}

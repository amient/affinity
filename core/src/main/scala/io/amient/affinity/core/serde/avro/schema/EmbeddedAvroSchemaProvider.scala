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

import scala.reflect.runtime.universe._

// TODO #9 This class is not fully Thread-Safe at the moment but once the API is completed it should be reimplemented.
trait EmbeddedAvroSchemaProvider extends AvroSchemaProvider {

  private val register = scala.collection.mutable.LinkedHashMap[Schema, (Class[_], Type)]()

  private var reg1: Map[Class[_], Int] = Map()

  private var reg2: Map[Int, (Type, Class[_], Schema)] = Map()

  private var reg3: Map[Schema, Int] = Map()

  override def schema(id: Int): (Type, Class[_], Schema) = reg2(id)

  override def schema(cls: Class[_]): Option[Int] = reg1.get(cls)

  override def schema(schema: Schema): Option[Int] = reg3.get(schema)

  /**
    * register run-time type and its new schema
    * @param schema
    * @param className
    */
  final def register(className: String, schema: Schema): Unit = synchronized {
    val (tpe, cls, currentSchema) = reg2(reg1(Class.forName(className)))
    register(tpe, cls,  schema)
  }

  /**
    * register compile-time type with its current schema
    * @param cls
    * @tparam T
    */
  final def register[T: TypeTag](cls: Class[T]): Unit = synchronized {
    register(typeOf[T], cls, AvroRecord.inferSchema(cls))
  }

  /**
    * register compile-time type with older schema version
    * @param schema
    * @param cls
    * @tparam T
    */
  final def register[T: TypeTag](cls: Class[T], schema: Schema): Unit = synchronized {
    register(typeOf[T], cls, schema)
  }

  private def register(tpe: Type, cls: Class[_], schema: Schema): Unit = synchronized {
    register += schema -> (cls, tpe)
    reg1 = register.zipWithIndex.map { case ((schema, (cls, tpe)), i) =>
      cls -> i
    }.toMap
    reg2 = register.zipWithIndex.map { case ((schema, (cls, tpe)), i) =>
      i -> (tpe, cls, schema)
    }.toMap
    reg3 = register.zipWithIndex.map { case ((schema, cls), i) =>
      schema -> i
    }.toMap
  }
}

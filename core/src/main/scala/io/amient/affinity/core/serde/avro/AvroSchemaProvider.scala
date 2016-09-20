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

package io.amient.affinity.core.serde.avro

import org.apache.avro.Schema

trait AvroSchemaProvider {

  private val reg1: Map[Class[_], Int] = register.zipWithIndex.map { case ((schema, cls), i) =>
    cls -> i
  }.toMap

  private val reg2: Map[Int, (Class[_], Schema)] = register.zipWithIndex.map { case ((schema, cls), i) =>
    i -> (cls, schema)
  }.toMap

  private val reg3: Map[Schema, Int] = register.zipWithIndex.map { case ((schema, cls), i) =>
    schema -> i
  }.toMap

  protected def register: Seq[(Schema, Class[_])]

  def schema(id: Int): (Class[_], Schema) = reg2(id)

  def schema(cls: Class[_]): Option[Int] = reg1.get(cls)

  def schema(schema: Schema): Option[Int] = reg3.get(schema)

}

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

package io.amient.affinity.core.data

import akka.serialization.JSerializer
import org.apache.avro.Schema

abstract class AvroSerde extends JSerializer {

  val reg1: Map[Class[_], Int] = register.zipWithIndex.map { case ((cls, schema), i) =>
    cls -> i
  }.toMap

  val reg2: Map[Int, Schema] = register.zipWithIndex.map { case ((cls, schema), i) =>
    i -> schema
  }.toMap

  def register: Seq[(Class[_], Schema)]

  override def identifier: Int = 21

  override protected def fromBinaryJava(bytes: Array[Byte], manifest: Class[_]): AnyRef = {
    fromBytes(bytes, manifest.asSubclass(classOf[AnyRef]))
  }

  override def toBinary(o: AnyRef): Array[Byte] = toBytes(o)

  override def includeManifest: Boolean = false

  def toBytes[T](obj: T): Array[Byte] = {
    reg1.get(obj.getClass) match {
      case None => throw new IllegalArgumentException("Avro schema not registered for " + obj.getClass)
      case Some(schemaId) =>
        val schema = reg2(schemaId)
        AvroRecord.write(obj, schema, schemaId)
    }
  }

  def fromBytes[T <: AnyRef](bytes: Array[Byte], cls: Class[T]): T = {
    AvroRecord.read(bytes, cls, (schemaId: Int) => reg2(schemaId))
  }


}

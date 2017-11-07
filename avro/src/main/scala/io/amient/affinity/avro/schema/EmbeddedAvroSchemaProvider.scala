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

import java.util.concurrent.ConcurrentHashMap

import org.apache.avro.Schema

import scala.collection.JavaConverters._

trait EmbeddedAvroSchemaProvider extends AvroSchemaProvider {

  protected val internal = new ConcurrentHashMap[Int, (Class[_], Schema)]()

//  override def getSchema(id: Int): Option[Schema] = {
//    internal.getOrDefault(id,null) match {
//      case null => None
//      case value => Some(value._2)
//    }
//  }

  override private[schema] def registerSchema(cls: Class[_], schema: Schema): Int = synchronized {
    val existing = internal.asScala.filter(_._2 == (cls, schema))
    if (existing.isEmpty) {
      val newId = internal.size
      internal.put(newId, (cls, schema))
      newId
    } else {
      existing.keys.max
    }
  }

  override private[schema] def getAllRegistered: List[(Int, Schema)] = {
    internal.asScala.mapValues(_._2).toList
  }

  override private[schema] def hypersynchronized[X](f: => X): X = synchronized(f)

}
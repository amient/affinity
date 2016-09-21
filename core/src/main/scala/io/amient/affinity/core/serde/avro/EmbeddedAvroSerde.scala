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

import io.amient.affinity.core.serde.Serde

abstract class EmbeddedAvroSerde extends Serde with AvroSchemaProvider {

  override def identifier: Int = 101

  override protected def fromBytes(bytes: Array[Byte]): AnyRef = AvroRecord.read(bytes, this)

  override protected def toBytes(obj: Any): Array[Byte] = {
    if (obj == null) null
    else obj match {
      //AvroRecords are capable of forward-compatible schema evolution
      case record: AvroRecord[_] => schema(record.getSchema) match {
        case None => throw new IllegalArgumentException("Avro schema not registered for " + obj.getClass)
        case Some(schemaId) => AvroRecord.write(obj, record.getSchema, schemaId)
      }
      case _ => schema(obj.getClass) match {
        case None => throw new IllegalArgumentException("Avro schema not registered for " + obj.getClass)
        case Some(schemaId) =>
          val (_, _, writerSchema) = schema(schemaId)
          AvroRecord.write(obj, writerSchema, schemaId)
      }
    }
  }


}

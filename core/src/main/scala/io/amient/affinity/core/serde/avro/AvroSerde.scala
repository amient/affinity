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
import io.amient.affinity.core.serde.avro.schema.AvroSchemaProvider
import org.apache.avro.generic.IndexedRecord

trait AvroSerde extends Serde with AvroSchemaProvider {

  override def identifier: Int = 101

  /**
    * Deserialize bytes to a concrete instance
    * @param bytes
    * @return AvroRecord for registered Type
    *         GenericRecord if no type is registered for the schema retrieved from the schemaRegistry
    *         null if bytes are null
    */
  override def fromBytes(bytes: Array[Byte]): Any = AvroRecord.read(bytes, this) /*match {
  TODO #16 check this works with the schema evolution
    case avroRecord: AvroRecord[_] => avroRecord
    case record: GenericRecord => null
  }*/

  /**
    * @param obj instance to serialize
    * @return serialized byte array
    */
  override def toBytes(obj: Any): Array[Byte] = {
    if (obj == null) null
    else obj match {
      case record: IndexedRecord => schema(record.getSchema) match {
        case None => throw new IllegalArgumentException("Avro schema not registered for " + obj.getClass)
        case Some(schemaId) => AvroRecord.write(obj, record.getSchema, schemaId)
      }
    }
  }


}

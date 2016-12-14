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


package io.amient.affinity.example.minimal

import akka.actor.ExtendedActorSystem
import io.amient.affinity.core.serde.Serde
import io.amient.affinity.core.serde.avro.schema.EmbeddedAvroSchemaProvider
import io.amient.affinity.core.serde.avro.{AvroRecord, AvroSerde}
import io.amient.affinity.core.util.Reply

class AvroSerdeForAkka(system: ExtendedActorSystem) extends Serde[Any]  {

  val cls = system.settings.config.getString("affinity.avro.register.class")
  //FIXME generalise avro serde constructors to Config
  val internal: AvroSerde = Class.forName(cls).newInstance().asInstanceOf[AvroSerde]

  //TODO finalize override def identifier: Int = 101

  override def fromBytes(bytes: Array[Byte]): Any = internal.fromBytes(bytes)

  override def toBytes(obj: Any): Array[Byte] = internal.toBytes(obj)

  override def close(): Unit = internal.close()

  override def identifier: Int = internal.identifier
}

class MyAvroSerde extends AvroSerde with EmbeddedAvroSchemaProvider {
    register(classOf[GetValue])
    register(classOf[PutValue])
}

case class GetValue(key: String) extends AvroRecord[GetValue] with Reply[Option[String]] {
  override def hashCode(): Int = key.hashCode()
}

case class PutValue(key: String, value: String) extends AvroRecord[PutValue] with Reply[Option[String]] {
  override def hashCode(): Int = key.hashCode()
}


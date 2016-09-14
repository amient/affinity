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

import java.util

import akka.serialization.JSerializer
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

trait Serde extends JSerializer with Serializer[Any] with Deserializer[Any] {

  protected def fromBytes(bytes: Array[Byte]): AnyRef

  protected def toBytes(obj: Any): Array[Byte]

  //////////////////////////////////////////////////////////////////////////

  override def includeManifest: Boolean = false

  override def toBinary(obj: AnyRef): Array[Byte] = toBytes(obj)

  override protected def fromBinaryJava(bytes: Array[Byte], manifest: Class[_]): AnyRef = fromBytes(bytes)

  override def serialize(topic: String, data: scala.Any): Array[Byte] = toBytes(data)

  override def deserialize(topic: String, data: Array[Byte]): Any = fromBytes(data)

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()


}

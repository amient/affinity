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

package io.amient.affinity.core.serde.primitive

import akka.actor.ExtendedActorSystem
import akka.serialization.{JSerializer, SerializationExtension}
import io.amient.affinity.core.util.ByteUtils

class OptionSerde(val system: ExtendedActorSystem) extends JSerializer {

  override protected def fromBinaryJava(bytes: Array[Byte], manifest: Class[_]): AnyRef = {
    if (bytes.length == 0) None else {
      val delegate = ByteUtils.asIntValue(bytes, bytes.length - 4)
      Some(SerializationExtension(system).serializerByIdentity(delegate).fromBinary(bytes))
    }
  }

  override def identifier: Int = 29

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case None => Array[Byte]()
    case Some(other: AnyRef) =>
      val delegate = SerializationExtension(system).findSerializerFor(other)
      val bytes = delegate.toBinary(other)
      val result = new Array[Byte](bytes.length + 4)
      Array.copy(bytes, 0, result, 0, bytes.length)
      ByteUtils.putIntValue(delegate.identifier, result, bytes.length)
      result
  }

  override def includeManifest: Boolean = {
    false
  }
}

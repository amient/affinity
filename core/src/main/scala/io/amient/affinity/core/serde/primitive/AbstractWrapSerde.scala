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

abstract class AbstractWrapSerde(val system: ExtendedActorSystem) extends JSerializer {

  def fromBinaryWrapped(bytes: Array[Byte], offset: Int = 0, len: Int = -1) = {
    val serializerIdentifier = ByteUtils.asIntValue(bytes, offset)
    val data = new Array[Byte](bytes.length - 4 - offset)
    Array.copy(bytes, 4 + offset, data, 0, (if (len == -1) bytes.length - offset else len) - 4)
    SerializationExtension(system).serializerByIdentity(serializerIdentifier).fromBinary(data)
  }

  def toBinaryWrapped(wrapped: AnyRef, offset: Int = 0): Array[Byte] = {
      val delegate = SerializationExtension(system).findSerializerFor(wrapped)
      val bytes = delegate.toBinary(wrapped)
      val result = new Array[Byte](bytes.length + 4 + offset)
      ByteUtils.putIntValue(delegate.identifier, result, 0)
      Array.copy(bytes, 0, result, 4 + offset, bytes.length)
      result
  }

  override def includeManifest: Boolean = false

}

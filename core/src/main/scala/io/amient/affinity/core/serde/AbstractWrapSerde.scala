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

package io.amient.affinity.core.serde

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import com.typesafe.config.Config
import io.amient.affinity.core.util.ByteUtils

abstract class AbstractWrapSerde(serdes: Serdes) extends Serializer {

  def this(system: ExtendedActorSystem) = this(Serde.tools(system))
  def this(config: Config) = this(Serde.tools(config))

  def fromBinaryWrapped(bytes: Array[Byte]): Any = {
    val serializerIdentifier = ByteUtils.asIntValue(bytes)
    val data = new Array[Byte](bytes.length - 4)
    Array.copy(bytes, 4, data, 0, bytes.length - 4)
    val wrappedSerde = serdes.by(serializerIdentifier)
    wrappedSerde.fromBytes(data)
  }

  def toBinaryWrapped(wrapped: Any, offset: Int = 0): Array[Byte] = {
    val w: AnyRef = wrapped match {
      case null => null
      case ref: AnyRef => ref
      case u: Unit => u.asInstanceOf[AnyRef]
      case z: Boolean => z.asInstanceOf[AnyRef]
      case b: Byte => b.asInstanceOf[AnyRef]
      case c: Char => c.asInstanceOf[AnyRef]
      case s: Short => s.asInstanceOf[AnyRef]
      case i: Int => i.asInstanceOf[AnyRef]
      case l: Long => l.asInstanceOf[AnyRef]
      case f: Float => f.asInstanceOf[AnyRef]
      case d: Double => d.asInstanceOf[AnyRef]
    }
    val delegate = serdes.find(w)
    val bytes: Array[Byte] = delegate.toBinary(w)
    val result = new Array[Byte](bytes.length + 4 + offset)
    ByteUtils.putIntValue(delegate.identifier, result, 0)
    Array.copy(bytes, 0, result, 4 + offset, bytes.length)
    result
  }

  override def includeManifest: Boolean = false

}

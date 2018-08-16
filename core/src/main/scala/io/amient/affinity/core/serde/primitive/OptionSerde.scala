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
import com.typesafe.config.Config
import io.amient.affinity.core.serde.{AbstractWrapSerde, Serde, Serdes}

class OptionSerde(serdes: Serdes) extends AbstractWrapSerde(serdes) with Serde[Option[Any]] {

  def this(system: ExtendedActorSystem) = this(Serde.tools(system))
  def this(config: Config) = this(Serde.tools(config))

  override protected def fromBytes(bytes: Array[Byte]): Option[Any] = {
    if (bytes.length == 0) None else Some(fromBinaryWrapped(bytes))
  }

  override def identifier: Int = 131

  override def toBytes(o: Option[Any]): Array[Byte] = o match {
    case None => Array[Byte]()
    case Some(other: AnyRef) => toBinaryWrapped(other)
    case _ => throw new NotImplementedError("AnyVal needs conversion to AnyRef")
  }

  override def includeManifest: Boolean = {
    false
  }

  override def close() = ()
}

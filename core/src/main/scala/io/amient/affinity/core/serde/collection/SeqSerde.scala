/*
 * Copyright 2016-2017 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.core.serde.collection

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import akka.actor.ExtendedActorSystem
import com.typesafe.config.Config
import io.amient.affinity.core.serde.{AbstractWrapSerde, Serde, Serdes}

class SeqSerde(serdes: Serdes) extends AbstractWrapSerde(serdes) with Serde[Seq[Any]] {

  def this(system: ExtendedActorSystem) = this(Serde.tools(system))
  def this(config: Config) = this(Serde.tools(config))

  override def identifier: Int = 141

  override def close(): Unit = ()

  override protected def fromBytes(bytes: Array[Byte]): Seq[Any] = {
    val di = new DataInputStream(new ByteArrayInputStream(bytes))
    val numItems = di.readInt()
    val result = ((1 to numItems) map { _ =>
      val len = di.readInt()
      val item = new Array[Byte](len)
      di.read(item)
      fromBinaryWrapped(item)
    }).toList
    di.close()
    result
  }

  override def toBytes(seq: Seq[Any]): Array[Byte] = {
    val os = new ByteArrayOutputStream()
    val d = new DataOutputStream(os)
    d.writeInt(seq.size)
    for (a: Any <- seq) a match {
      case ref: AnyRef =>
        val item = toBinaryWrapped(ref)
        d.writeInt(item.length)
        d.write(item)
    }
    os.close
    os.toByteArray
  }
}

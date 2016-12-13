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

import io.amient.affinity.core.serde.Serde
import io.amient.affinity.core.util.ByteUtils

class IntSerde extends Serde[Int] {

  override def identifier: Int = 20

  override def fromBytes(bytes: Array[Byte]): Int = ByteUtils.asIntValue(bytes)

  override def toBytes(obj: Int): Array[Byte] = ByteUtils.putIntValue(obj, new Array[Byte](4),0)

  override def close(): Unit = ()

}

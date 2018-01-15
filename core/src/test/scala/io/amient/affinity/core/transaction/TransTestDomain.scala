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

package io.amient.affinity.core.transaction

import io.amient.affinity.avro.AvroRecord
import io.amient.affinity.core.actor.Routed
import io.amient.affinity.core.util.Reply

case class TestKey(val id: Int) extends AvroRecord with Routed with Reply[Option[TestValue]] {
  override def key = this
}

case class TestValue(items: List[Int]) extends AvroRecord {
  def withAddedItem(item: Int) = TestValue(items :+ item)
  def withRemovedItem(item: Int) = TestValue(items.filter(_ != item))
}

case class AddItem(key: TestKey, item: Int) extends AvroRecord with Routed with Instruction[TestValue] {
  override def reverse(result: TestValue): Option[Instruction[_]] = {
    if (result.items.contains(item)) None else Some(RemoveItem(key, item))
  }
}

case class RemoveItem(key: TestKey, item: Int) extends AvroRecord with Routed with Instruction[TestValue] {
  override def hashCode() = key.hashCode()
  override def reverse(result: TestValue): Option[Instruction[_]] = {
    if (result.items.contains(item)) Some(AddItem(key, item)) else None
  }
}


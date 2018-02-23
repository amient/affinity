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

import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.{Partition, Routed}
import io.amient.affinity.core.util.Reply

case class GetValue(key: String) extends AvroRecord with Routed with Reply[Option[String]]

case class PutValue(key: String, value: String) extends AvroRecord with Routed with Reply[Option[String]]

class ExamplePartition extends Partition {

  val cache = state[String, String]("cache")

  import context.dispatcher

  override def handle: Receive = {
    case request @ GetValue(key: String) => sender.reply(request) {
      cache(key)
    }

    case request @ PutValue(key: String, value: String) => sender.replyWith(request) {
      cache.update(key, value)
    }
  }

}

/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.example

import io.amient.affinity.core.actor.GatewayStream
import io.amient.affinity.core.storage.Record

import scala.concurrent.Future

class ExampleWordCount extends GatewayStream {

  val out = output[String, Long]("output-stream")

  val counter = global[String, Long]("state-counter")

  private implicit val executor = scala.concurrent.ExecutionContext.Implicits.global

  input[Array[Byte], Array[Byte]]("input-stream") { record =>
    //raw input stream: keys are null, and values are plain text bytes
    val words = new String(record.value).split("\\s").toList
    val updates = words.map { word =>
      //this operation is not idempotent so due to at-least-once guarantee, repeated inputs may be double-counted
      counter.updateAndGet(word, {
        case None => Some(1)
        case Some(prev) => Some(prev + 1)
      }) flatMap {
        updatedCount => out.append(new Record(word, updatedCount.get, record.timestamp))
        //any records written to out will be flushed automatically - gateway manages all declared outputs
      }
    }
    // however to get end-to-end processing guarantee we need to return the ack from counter updates
    // which is here summarized as Future.sequence of all individual word count updates
    // as part of processing the record it will be added to the pool of futures that need complete when commit occurs
    Future.sequence(updates)
  }

}

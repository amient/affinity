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

import scala.concurrent.Future

class ExampleSSP extends GatewayStream {

  val out = output[String, Long]("output-stream")

  val counter = global[String, Long]("state-counter")

  input[Null, String]("input-stream") {
    record =>
      val ack: Future[Unit] = counter.updateAndGet(record.value, current => current match {
        case None => Some(1)
        case Some(prev) => Some(prev + 1)
      }).collect {
        case Some(updatedCount) => out.write(Iterator.single((record.value, updatedCount)))
      }(scala.concurrent.ExecutionContext.Implicits.global)

      //TODO #144 add guarantees - at the moment
  }

}

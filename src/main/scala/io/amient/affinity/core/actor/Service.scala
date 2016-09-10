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

package io.amient.affinity.core.actor

import akka.actor.Actor
import akka.event.Logging
import io.amient.affinity.core.actor.Node.{ServiceOffline, ServiceOnline}

trait Service extends Actor {

  val log = Logging.getLogger(context.system, this)

  val cluster = context.actorSelection("/user/controller/gateway/cluster")

  override def preStart(): Unit = {
    log.info("Starting service: " + self.path.name)
    context.parent ! ServiceOnline(self)
    super.preStart()
  }

  override def postStop(): Unit = {
    log.info("Stopping service: " + self.path.name)
    context.parent ! ServiceOffline(self)
    super.postStop()
  }


}


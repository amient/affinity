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
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Container.{ServiceOffline, ServiceOnline}
import io.amient.affinity.core.util.Reply

object Service {

  case class BecomeStandby() extends Reply[Unit]

  case class BecomeMaster() extends Reply[Unit]

}

trait Service extends Actor {

  private val log = Logging.getLogger(context.system, this)

  import Service._

  protected def onBecomeMaster: Unit = ()

  protected def onBecomeStandby: Unit = ()

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

  final override def receive: Receive = manage orElse handle

  protected def manage: Receive = {
    case msg@BecomeMaster() => sender.reply(msg) {
      onBecomeMaster
    }
    case msg@BecomeStandby() => sender.reply(msg) {
      onBecomeStandby
    }
  }

  def handle: Receive

}


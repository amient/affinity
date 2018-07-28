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

package io.amient.affinity.core.actor

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import io.amient.affinity.Conf

trait ActorHandler extends Actor {

  val logger = Logging.getLogger(context.system, this)

  final val conf = Conf(context.system.settings.config)

  private var suspended: Boolean = true

  private val suspendedQueueMaxSize = conf.Affi.Node.SuspendQueueMaxSize()

  private val suspendedHttpRequestQueue = scala.collection.mutable.ListBuffer[Any]()

  def isSuspended = suspended

  final override def receive: Receive = manage orElse hold orElse handle orElse unhandled

  def manage: Receive = PartialFunction.empty

  def suspend: Unit = suspended = true

  final def hold: Receive = {
    case message: Any if (suspended) => onHold(message, sender)
  }

  def onHold(message: Any, sender: ActorRef): Unit = {
    logger.warning("Suspended handling: " + message)
    if (suspendedHttpRequestQueue.size < suspendedQueueMaxSize) {
      suspendedHttpRequestQueue += message
    } else {
      new RuntimeException("Suspension queue overflow")
    }

  }

  def resume: Unit = {
    suspended = false
    val reprocess = suspendedHttpRequestQueue.toList
    suspendedHttpRequestQueue.clear
    if (reprocess.length > 0) logger.info(s"Re-processing ${reprocess.length} suspended http requests")
    reprocess.foreach(handle(_))
  }

  def handle: Receive = PartialFunction.empty

  def unhandled: Receive = {
    case any => logger.warning(s"Unhandled message $any")
  }

}

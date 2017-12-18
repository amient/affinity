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

import java.util.{Observable, Observer}

import akka.actor.{Actor, ActorRef, Props, Status}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Container.{ServiceOffline, ServiceOnline}
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.util.Reply

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

object Partition {
  final val INTERNAL_CREATE_KEY_VALUE_MEDIATOR = "INTERNAL_CREATE_KEY_VALUE_MEDIATOR"
  case class BecomeStandby() extends Reply[Unit]

  case class BecomeMaster() extends Reply[Unit]
}

trait Partition extends ActorHandler with ActorState {

  import Partition._

  private val log = Logging.getLogger(context.system, this)

  /**
    * service container name
    */
  implicit val keyspace = self.path.parent.name

  /**
    * physical partition id
    */
  implicit val partition = self.path.name.toInt

  /**
    * onBecomeMaster is signalling that the partition should take over the responsibility
    * of being the Master for the related physical partition. The signalling message
    * may be resent as part of ack contract so this method must be idempotent.
    */
  protected def onBecomeMaster: Unit = {
    bootState()
    log.debug(s"Became master for partition $keyspace/$partition")
  }

  /**
    * onBecomeStandby is signalling that the partition should become a passive standby
    * and keep listening to the changes in the related physical partition.
    * The signalling message may be resent as part of ack contract so this method must be idempotent.
    */
  protected def onBecomeStandby: Unit = {
    tailState()
    log.debug(s"Became standby for partition $keyspace/$partition")
  }

  override def preStart(): Unit = {
    log.debug(s"Starting partition: $keyspace/$partition")
    context.parent ! ServiceOnline(self)
    super.preStart()
  }

  override def postStop(): Unit = {
    log.debug(s"Stopping partition: $keyspace/$partition")
    context.parent ! ServiceOffline(self)
    super.postStop()
  }

  abstract override def manage: Receive = super.manage orElse {

    case msg@BecomeMaster() =>
      sender.reply(msg) {}
      onBecomeMaster

    case msg@BecomeStandby() =>
      sender.reply(msg) {}
      onBecomeStandby

    case (key: Any, INTERNAL_CREATE_KEY_VALUE_MEDIATOR, stateStoreName: String) => try {
      val state = getStateStore(stateStoreName)
      sender ! context.actorOf(Props(new KeyValueMediator(state, key)))
    } catch {
      case NonFatal(e) => sender ! Status.Failure(e)
    }
  }
}

class KeyValueMediator(state: State[_, _], key: Any) extends Actor {

  private var observer: Option[Observer] = None

  import context.dispatcher

  implicit val scheduler = context.system.scheduler

  override def postStop(): Unit = {
    observer.foreach(state.removeKeyValueObserver(key, _))
  }

  override def receive: Receive = {
    case frontend: ActorRef => createKeyValueObserver(key, frontend)
  }

  def createKeyValueObserver(key: Any, frontend: ActorRef): Unit = {
    observer = Some(state.addKeyValueObserver(key, new Observer() {
      override def update(o: Observable, arg: scala.Any): Unit = {
        val t = 1 seconds
        implicit val timeout = Timeout(t)
        //here is the ack the requires the actorPublisher at the gateway side to ack the receipt
        //TODO write a system test that verifies that websocket actor publisher sends a unit ack
        (frontend ? arg) recover {
          case any =>
            //in case of any error the mediator will be stopped and also any front end that watches it
            context.stop(self)
        }
      }
    }))
  }


}


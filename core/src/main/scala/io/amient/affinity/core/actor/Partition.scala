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

import java.util.{Observable, Observer, Optional}

import akka.actor.{Actor, ActorRef, Props, Status}
import akka.event.Logging
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Container.{PartitionOffline, PartitionOnline}
import io.amient.affinity.core.actor.Partition.RegisterMediatorSubscriber
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.util.Reply

import scala.language.postfixOps
import scala.util.control.NonFatal

object Partition {
  //FIXME #122 make special serializer for mediation messages - currently this is java and the footprint is huge
  case class CreateKeyValueMediator(stateStore: String, key: Any) extends Routed
  case class KeyValueMediatorCreated(mediator: ActorRef)
  case class RegisterMediatorSubscriber(subscriber: ActorRef)

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

  override def preStart(): Unit = {
    log.debug(s"Starting keyspace: $keyspace, partition: $partition")
    //active state will block until all state stores have caught-up
    activeState()
    //only after states have caught up we make the partition online and available to cooridnators
    context.parent ! PartitionOnline(self)
    super.preStart()
  }

  /**
    * onBecomeMaster will be fired when this Partition Actor has successfully become the Master for the
    * underlying physical partition and after all state stores have been caught-up and are at this
    * point consistent with the storage.
    */
  protected def onBecomeMaster: Unit = ()

  /**
    * onBecomeStandby will be fired when this Partition Actor became a standby for the underlying
    * physical partition and after all state stores have been switched to the background passive mode.
    */
  protected def onBecomeStandby: Unit = ()

  override def postStop(): Unit = {
    try {
      log.debug(s"Stopping keyspace: $keyspace, partition: $partition")
      context.parent ! PartitionOffline(self)
    } finally super.postStop()
  }

  abstract override def manage: Receive = super.manage orElse {

    case msg@BecomeMaster() =>
      sender.reply(msg) {} //acking the receipt of the instruction immediately
      activeState() //then blocking the inbox until state stores have caught-up with storage
      log.debug(s"Became master for partition $keyspace/$partition")
      onBecomeMaster //then invoke custom handler

    case msg@BecomeStandby() =>
      sender.reply(msg) {} //acking the receipt of the instruction immediately
      passiveState()  //then switch state stores to passive mode, i.e. tailing the storage in the background
      log.debug(s"Became standby for partition $keyspace/$partition")
      onBecomeStandby

    case CreateKeyValueMediator(stateStoreName: String, key: Any) => try {
      val state = getStateStore(stateStoreName)
      val props = state.uncheckedMediator(self, key)
      sender ! KeyValueMediatorCreated(context.actorOf(props))
    } catch {
      case NonFatal(e) => sender ! Status.Failure(e)
    }
  }
}

class KeyValueMediator[K](partition: ActorRef, state: State[K, _], key: K) extends Actor {

  private var observer: Option[Observer] = None

  implicit val scheduler = context.system.scheduler

  override def postStop(): Unit = {
    observer.foreach(state.removeKeyValueObserver(key, _))
  }

  override def receive: Receive = {
    case RegisterMediatorSubscriber(subscriber:ActorRef) => createKeyValueObserver(key, subscriber)
    case forward: Any => partition.tell(forward, sender)
  }

  def createKeyValueObserver(key: K, frontend: ActorRef): Unit = {

    observer = Some(new Observer {
      override def update(o: Observable, arg: scala.Any): Unit = {
        frontend ! arg
      }
    })
    val observable = state.addKeyValueObserver(key, observer.get)

    // send initial value on subscription TODO - maybe this is up to the client, e.g. websocket impl., to decide
    observer.foreach(_.update(observable, state.get(key)))
  }


}


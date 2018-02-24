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

import java.util.concurrent.CopyOnWriteArrayList
import java.util.{Observable, Observer}

import akka.actor.{Actor, ActorRef, Status}
import akka.event.Logging
import io.amient.affinity.Conf
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Container.{PartitionOffline, PartitionOnline}
import io.amient.affinity.core.actor.Partition.RegisterMediatorSubscriber
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.util.Reply

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.control.NonFatal

object Partition {
  //FIXME #122 make special serializer for mediation messages - currently this is java and the footprint is huge
  case class CreateKeyValueMediator(stateStore: String, key: Any) extends Routed
  case class KeyValueMediatorCreated(mediator: ActorRef)
  case class RegisterMediatorSubscriber(subscriber: ActorRef)

  case class BecomeStandby() extends Reply[Unit]

  case class BecomeMaster() extends Reply[Unit]
}

trait Partition extends ActorHandler {

  import Partition._

  private val log = Logging.getLogger(context.system, this)

  /**
    * keyspace identifier
    */
  implicit val keyspace = self.path.parent.name

  /**
    * partition id
    */
  implicit val partition = self.path.name.toInt

  private var started = false
  private val declaredStateStores: CopyOnWriteArrayList[(String, State[_, _])] = new CopyOnWriteArrayList[(String, State[_, _])]()
  private lazy val stateStores: Map[String, State[_, _]] = declaredStateStores.iterator().asScala.toMap

  def state[K: ClassTag, V: ClassTag](store: String)(implicit keyspace: String, partition: Int): State[K, V] = {
    if (started) throw new IllegalStateException("Cannot declare state after the actor has started")
    val identifier = if (partition < 0) store else s"$keyspace-$store-$partition"
    val conf = Conf(context.system.settings.config)
    val numPartitions = conf.Affi.Keyspace(keyspace).NumPartitions()
    val stateConf = conf.Affi.Keyspace(keyspace).State(store)
    val state = State.create[K, V](identifier, partition, stateConf, numPartitions, context.system)
    declaredStateStores.add((store, state))
    state
  }


  override def preStart(): Unit = {
    started = true
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
      closeStateStores()
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

  private[core] def getStateStore(stateStoreName: String): State[_, _] = {
    if (!started) throw new IllegalStateException("Cannot get state store rererence before the actor has started")
    stateStores(stateStoreName)
  }

  private[core] def activeState(): Unit = {
    stateStores.values.foreach { state =>
      state.boot
      if (state.external) state.tail
    }
  }

  private[core] def passiveState(): Unit = {
    stateStores.values.foreach(_.tail)
  }

  private[core] def closeStateStores(): Unit = stateStores.foreach {
    case (id, state) => try state.close catch {
      case NonFatal(e) => log.error(e, s"Could not close store $id")
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


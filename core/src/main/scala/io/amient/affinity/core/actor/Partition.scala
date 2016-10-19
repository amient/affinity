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

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.http.scaladsl.model.ws.Message
import akka.http.scaladsl.model.ws.TextMessage.Strict
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Partition.Observe
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.util.Reply

import scala.concurrent.duration._

object Partition {

  //TODO use protobuf for internal messages that extend Reply but not say AvroRecord
  final case class Observe(stateStoreName: String, key: Any) extends Reply[ActorRef] {
    override def hashCode(): Int = key.hashCode
  }

  final case class WSMessage(msg: Message) extends Reply[Unit]

}

trait Partition extends Service with ActorState {

  private val log = Logging.getLogger(context.system, this)

  /**
    * physical partition id - this is read from the name of the Partition Actor;  assigned by the Region
    */
  implicit val partition = self.path.name.toInt

  /**
    * onBecomeMaster is signalling that the partition should take over the responsibility
    * of being the Master for the related physical partition. The signalling message
    * may be resent as part of ack contract so this method must be idempotent.
    */
  override protected def onBecomeMaster: Unit = {
    bootState()
    log.info(s"Became master for partition $partition")
  }

  /**
    * onBecomeStandby is signalling that the partition should become a passive standby
    * and keep listening to the changes in the related physical partition.
    * The signalling message may be resent as part of ack contract so this method must be idempotent.
    */
  override protected def onBecomeStandby: Unit = {
    tailState()
    log.info(s"Became standby for partition $partition")
  }

  override def postStop(): Unit = {
    super.postStop()
    log.info(s"Closing all state in partition $partition")
    closeState()
  }

  override protected def manage: Receive = super.manage orElse {
    case request@Observe(stateStoreName, key) => sender.reply(request) {
      val state = getStateStore(stateStoreName)
      context.actorOf(Props(new ChangeStream(state, key)))
    }
  }
}

class ChangeStream(state: State[_, _], key: Any) extends Actor {

  private var observer: Option[Observer] = None

  import context.dispatcher
  implicit val scheduler = context.system.scheduler

  override def postStop(): Unit = {
    observer.foreach(state.removeObserver(key, _))
  }

  override def receive: Receive = {
    case frontend: ActorRef => addWebSocketObserver(key, frontend)
    //case tm: TextMessage => //TODO look into using websocket client json messages
    //case bm: BinaryMessage => //TODO end-to-end avro with js websocket client holding schema registry and using BinaryMessage
  }

  def addWebSocketObserver(key: Any, frontend: ActorRef): Unit = {
    observer = Some(state.addObserver(key, new Observer() {
      override def update(o: Observable, arg: scala.Any): Unit = {
        //TODO end-to-end avro with js websocket client holding schema registry and using BinaryMessage
        val textRepr = arg match {
          case None => ""
          case Some(value) => Encoder.json(value)
          case other => Encoder.json(other)
        }

        val wsMessage = Partition.WSMessage(new Strict(textRepr))

        val t = 1 seconds
        implicit val timeout = Timeout(t)
        frontend ack wsMessage onFailure {
          case any => context.stop(self)
        }
      }
    }))
  }


}


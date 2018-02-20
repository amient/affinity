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

import akka.actor.Actor
import akka.event.Logging
import io.amient.affinity.Conf
import io.amient.affinity.core.storage.{State, StateConf}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait ActorState extends Actor {

  private val log = Logging.getLogger(context.system, this)

  private val storageRegistry = new CopyOnWriteArrayList[(String, State[_, _])]()

  abstract override def postStop(): Unit = {
    super.postStop()
    closeState()
    storageRegistry.clear()
  }

  def state[K: ClassTag, V: ClassTag](store: String)(implicit keyspace: String, partition: Int): State[K, V] = {
    state[K, V](store, {
      val identifier = if (partition < 0) store else s"$keyspace-$store-$partition"
      val conf = Conf(context.system.settings.config)
      val numPartitions = conf.Affi.Keyspace(keyspace).NumPartitions()
      val stateConf = conf.Affi.Keyspace(keyspace).State(store)
      State.create[K, V](identifier, partition, stateConf, numPartitions, context.system)
    })
  }

  private[core] def state[K: ClassTag, V: ClassTag](globalStore: String, conf: StateConf): State[K, V] = {
    state[K, V](globalStore, {
      State.create[K, V](globalStore, 0, conf, 1, context.system)
    })
  }

  private[core] def state[K, V](name: String, creator: => State[K, V]): State[K, V] = {
    val result = creator
    storageRegistry.add((name, result))
    result
  }

  private[core] def getStateStore(stateStoreName: String): State[_, _] = {
    storageRegistry.asScala.find(_._1 == stateStoreName).get._2
  }

  private[core] def bootState(): Unit = storageRegistry.asScala.foreach(_._2.boot)

  private[core] def tailState(): Unit = storageRegistry.asScala.foreach(_._2.tail)

  private[core] def closeState(): Unit = storageRegistry.asScala.foreach {
    case (id, state) => try state.close catch {
      case NonFatal(e) => log.error(e, s"Could not close store $id")
    }
  }

}

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
import io.amient.affinity.core.storage.State

import scala.collection.JavaConverters._

trait ActorState extends Actor {

  private val log = Logging.getLogger(context.system, this)

  private val storageRegistry = new CopyOnWriteArrayList[State[_, _]]()

  def state[K, V](creator: => State[K, V]): State[K, V] = {
    val stateStorage: State[K, V] = creator
    stateStorage.storage.init()
    stateStorage.storage.boot()
    stateStorage.storage.tail()
    storageRegistry.add(stateStorage)
    stateStorage
  }

  def bootState(): Unit = storageRegistry.asScala.foreach(_.storage.boot())

  def tailState(): Unit = storageRegistry.asScala.foreach(_.storage.tail())

  def closeState(): Unit = storageRegistry.asScala.foreach (_.storage.close())

}

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
import scala.collection.mutable._

abstract class Partition extends Actor {

  val log = Logging.getLogger(context.system, this)

  val id = self.path.name.split("-").last.toInt

  //TODO this is a prototype for the embedded storage API
  private val store = scala.collection.mutable.Map[String, Map[_, _]]()
  protected def storage[K,V](keyspace:String): scala.collection.mutable.Map[K, V] = {
    val storage = scala.collection.mutable.Map[K, V]()
    store += (keyspace -> storage)
    storage
  }

  final override def postStop(): Unit = {
    log.info("Committing Partition ChangeLog: " + self.path.name)
    //TODO commit change log
    //TODO clear state (if this is not a new instance)
    context.parent ! Region.PartitionOffline(self)
    super.postStop()
  }

  final override def preStart(): Unit = {
    log.info("Bootstrapping partition: " + self.path.name)
    //TODO bootstrap data
    Thread.sleep(1500)
    context.parent ! Region.PartitionOnline(self)
  }

}


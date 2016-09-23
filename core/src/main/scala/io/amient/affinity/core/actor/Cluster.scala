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

import java.util.concurrent.ConcurrentHashMap

import akka.actor.Actor
import akka.routing.{ActorRefRoutee, AddRoutee, RemoveRoutee, Routee}
import io.amient.affinity.core.util.ObjectHashPartitioner

object Cluster {
  final val CONFIG_NUM_PARTITIONS = "affinity.cluster.num.partitions"
}

class Cluster extends Actor {

  private val config = context.system.settings.config

  private val numPartitions = config.getInt(Cluster.CONFIG_NUM_PARTITIONS)

  //TODO #6 use lighter structure than concurrenthashmap
  private val currentRouteMap = new ConcurrentHashMap[Int, Routee]()

  //TODO configurable partitioner
  val partitioner = new ObjectHashPartitioner

  override def receive: Receive = {
    case AddRoutee(routee: ActorRefRoutee) =>
      /**
        * relying on Region to assign partition name equal to physical partition id
        */
      val partition = routee.ref.path.name.toInt
      currentRouteMap.put(partition, routee)

    case RemoveRoutee(routee: ActorRefRoutee) =>
      val partition = routee.ref.path.name.toInt
      currentRouteMap.put(partition, routee)

    case message => route(message)

  }

  private def route(message: Any): Unit = {
    val p = message match {
      case (k, v) => partitioner.partition(k, numPartitions)
      case v => partitioner.partition(v, numPartitions)
    }

    /**
      * This means that no region has registered the partition - this may happen for 2 reasons:
      * 1. all regions representing that partition are genuinely down and there nothing that can be done
      * 2. between a master failure and a standby election there may be a brief period
      *    of the partition not being represented -
      */
    //TODO #6 the second case needs to be handled by the API and there a different ways how to do it

    if (!currentRouteMap.containsKey(p)) {
      throw new IllegalStateException(s"Data partition `$p` is not represented in the cluster")
    }

    currentRouteMap.get(p).send(message, sender)

  }


}

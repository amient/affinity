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
import akka.routing._
import com.typesafe.config.Config
import io.amient.affinity.core.ack
import io.amient.affinity.core.config.CfgStruct
import io.amient.affinity.core.storage.StateConf
import io.amient.affinity.core.util.{ObjectHashPartitioner, Reply}

import scala.collection.mutable

object Keyspace {

  object KeyspaceConf extends KeyspaceConf {
    override def apply(config: Config): KeyspaceConf = new KeyspaceConf().apply(config)
  }

  class KeyspaceConf extends CfgStruct[KeyspaceConf] {
    val PartitionClass = cls("class", classOf[Partition], true)
    val NumPartitions = integer("num.partitions", true)
    val State = group("state", classOf[StateConf], false)
  }

  final case class CheckServiceAvailability(group: String) extends Reply[ServiceAvailability]
  final case class ServiceAvailability(group: String, suspended: Boolean)

}

class Keyspace(config: Config) extends Actor {

  import Keyspace._

  val appliedConfig = Keyspace.KeyspaceConf(config)

  private val numPartitions = appliedConfig.NumPartitions()

  private val routes = mutable.Map[Int, ActorRefRoutee]()

  val partitioner = new ObjectHashPartitioner

  override def receive: Receive = {

    /**
      * relying on Region to assign partition name equal to physical partition id
      */

    case AddRoutee(routee: ActorRefRoutee) =>
      val partition = routee.ref.path.name.toInt
      routes.put(partition, routee)
      routes.size == numPartitions

    case RemoveRoutee(routee: ActorRefRoutee) =>
      val partition = routee.ref.path.name.toInt
      routes.remove(partition) foreach { removed =>
        if (removed != routee) routes.put(partition, removed)
      }

    case req@CheckServiceAvailability(group) => sender.reply(req) {
      ServiceAvailability(group, suspended = (routes.size != numPartitions))
    }

    case GetRoutees => sender ! Routees(routes.values.toIndexedSeq)

    case message => getRoutee(message).send(message, sender)

  }

  private def getRoutee(message: Any): ActorRefRoutee = {
    val partition = message match {
      case (t1,_) => partitioner.partition(t1, numPartitions)
      case (t1,_, _) => partitioner.partition(t1, numPartitions)
      case (t1,_, _, _) => partitioner.partition(t1, numPartitions)
      case (t1,_, _, _, _) => partitioner.partition(t1, numPartitions)
      case (t1,_, _, _, _, _) => partitioner.partition(t1, numPartitions)
      case (t1,_, _, _, _, _, _) => partitioner.partition(t1, numPartitions)
      case (t1,_, _, _, _, _, _, _) => partitioner.partition(t1, numPartitions)
      case (t1,_, _, _, _, _, _, _, _) => partitioner.partition(t1, numPartitions)
      case (t1,_, _, _, _, _, _, _, _, _) => partitioner.partition(t1, numPartitions)
      case (t1,_, _, _, _, _, _, _, _, _, _) => partitioner.partition(t1, numPartitions)
      case v => partitioner.partition(v, numPartitions)
    }

    routes.get(partition) match {
      case Some(routee) => routee
      case None =>
        throw new IllegalStateException(s"Partition $partition is not represented in the cluster")
      /**
        * This means that no region has registered the partition which may happen for 2 reasons:
        * 1. all regions representing that partition are genuinely down and not coming back
        * 2. between a master failure and a standby takeover there may be a brief period
        * of the partition not being represented.
        *
        * Both of the cases will see IllegalStateException which have to be handled by ack-and-retry
        */

    }
  }


}

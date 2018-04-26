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

import java.lang

import akka.actor.Actor
import akka.actor.Status.Failure
import akka.routing._
import akka.serialization.SerializationExtension
import com.typesafe.config.Config
import io.amient.affinity.core.config.CfgStruct
import io.amient.affinity.core.storage.StateConf
import io.amient.affinity.core.util.{Reply, ScatterGather}
import io.amient.affinity.core.{Murmur2Partitioner, ack}

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal

object Keyspace {

  object KeyspaceConf extends KeyspaceConf {
    override def apply(config: Config): KeyspaceConf = new KeyspaceConf().apply(config)
  }

  class KeyspaceConf extends CfgStruct[KeyspaceConf] {
    val PartitionClass = cls("class", classOf[Partition], true)
      .doc("Implementation of core.actor.Partition of whose instances is the Keyspace composed")

    val NumPartitions = integer("num.partitions", true)
      .doc("Total number of partitions in the Keyspace")

    val State = group("state", classOf[StateConf], false).doc("Keyspace may have any number of States, each identified by its ID - each state within a Keyspace is co-partitioned identically")
  }

  final case class CheckKeyspaceStatus(_keyspace: String) extends Reply[KeyspaceStatus]

  final case class KeyspaceStatus(_keyspace: String, suspended: Boolean)

}

trait Routed {
  def key: Any
}

class Keyspace(config: Config) extends Actor {

  import Keyspace._

  val appliedConfig = Keyspace.KeyspaceConf(config)

  private val numPartitions = appliedConfig.NumPartitions()

  private val routes = mutable.Map[Int, ActorRefRoutee]()

  val serialization = SerializationExtension(context.system)

  val partitioner = new Murmur2Partitioner

  import context.dispatcher

  override def receive: Receive = {

    case message: Routed => try {
      getRoutee(message.key).send(message, sender)
    } catch {
      case NonFatal(e) => sender ! Failure(new RuntimeException(s"Could not route $message", e))
    }

    case req@ScatterGather(message: Reply[Any], t) => req(sender) ! {
      val recipients = routes.values
      implicit val timeout = t
      implicit val scheduler = context.system.scheduler
      Future.sequence(recipients.map(x => x.ref ?! message))
    }

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

    case req@CheckKeyspaceStatus(_keyspace) =>
      req(sender) ! KeyspaceStatus(_keyspace, suspended = (routes.size != numPartitions))

    case GetRoutees => sender ! Routees(routes.values.toIndexedSeq)

  }

  private def getRoutee(key: Any): ActorRefRoutee = {

    val routableKey: AnyRef = key match {
      case ref: AnyRef => ref
      case b: Byte => new lang.Byte(b)
      case c: Char => new lang.Character(c)
      case z: Boolean => new lang.Boolean(z)
      case s: Short => new lang.Short(s)
      case i: Int => new lang.Integer(i)
      case l: Long => new lang.Long(l)
      case f: Float => new lang.Float(f)
      case d: Double => new lang.Double(d)
    }

    val serializedKey = serialization.serialize(routableKey).get
    val partition = partitioner.partition(serializedKey, numPartitions)

    //log.trace(serializedKey.mkString(".") + " over " + numPartitions + " to " + partition)

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

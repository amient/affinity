package io.amient.affinity.core.actor

import akka.actor.Status.Failure
import akka.routing.{ActorRefRoutee, GetRoutees, Routees}
import akka.serialization.SerializationExtension
import io.amient.affinity.core.cluster.Coordinator
import io.amient.affinity.core.cluster.Coordinator.MembershipUpdate
import io.amient.affinity.core.util.{Reply, ScatterGather}
import io.amient.affinity.core.{Partitioner, ack, any2ref}

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal

trait Routed {
  def key: Any
}

final case class GroupStatus(identifier: String, suspended: Boolean)

class Group(identifier: String, numPartitions: Int, partitioner: Partitioner) extends ActorHandler {

  private val routees = mutable.Map[Int, ActorRefRoutee]()

  val serialization = SerializationExtension(context.system)

  implicit val executor = context.dispatcher

  val coordinator = Coordinator.create(context.system, identifier)

  override def preStart(): Unit = {
    super.preStart()
    // set up a watch for each referenced group coordinator
    // coordinator will be sending MembershipUpdate(..) messages for this group whenever members go online or offline
    // is added or removed to/from the routing tables
    coordinator.watch(self)
  }

  override def postStop(): Unit = try {
    try {
      coordinator.unwatch(self)
      coordinator.close()
    } catch {
      case NonFatal(e) => logger.warning(s"Could not close coordinator for group: $identifier", e);
    }
  } finally {
    super.postStop()
  }

  override def manage: Receive = super.manage orElse {

    case GetRoutees => sender ! Routees(routees.values.toIndexedSeq)

    case request: MembershipUpdate => request(sender) ! {
      val (add, remove) = request.mastersDelta(routees.map(_._2.ref).toSet)
      remove.foreach { routee =>
        val partition = routee.path.name.toInt
        routees.remove(partition) foreach { removed =>
          if (removed != routee) routees.put(partition, removed)
        }
      }
      add.foreach { routee =>
        val partition = routee.path.name.toInt
        routees.put(partition, ActorRefRoutee(routee))
        routees.size == numPartitions
      }
      evaluateSuspensionStatus()
    }
  }

  override def handle: Receive = {

    case message: Routed => try {
      getRoutee(message.key).send(message, sender)
    } catch {
      case NonFatal(e) => sender ! Failure(new RuntimeException(s"Could not route $message", e))
    }

    case req@ScatterGather(message: Reply[Any], t) => req(sender) ! {
      val recipients = routees.values
      implicit val timeout = t
      implicit val scheduler = context.system.scheduler
      Future.sequence(recipients.map(x => x.ref ?! message))
    }

    case other: Any => sender ! Failure(new RuntimeException(s"Message addressed to Group must be either Routed or ScatterGather: $other"))

  }

  private def evaluateSuspensionStatus() = {
    val shouldBeSuspended = routees.size != numPartitions
    if (shouldBeSuspended != isSuspended) {
      if (shouldBeSuspended) suspend else resume
      //parent will be a gateway which needs to collate all suspension states to a single flag
      context.parent ! GroupStatus(identifier, isSuspended)
    }
  }

  private def getRoutee(key: Any): ActorRefRoutee = {

    val serializedKey = serialization.serialize(any2ref(key)).get
    val partition = partitioner.partition(serializedKey, numPartitions)

    //log.trace(serializedKey.mkString(".") + " over " + numPartitions + " to " + partition)

    routees.get(partition) match {
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

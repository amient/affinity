package io.amient.akkahttp.actor

import java.util.Properties

import akka.actor.{Actor, ActorPath, ActorRef, Props}
import akka.event.Logging
import io.amient.akkahttp.Coordinator

object Region {
  final val CONFIG_AKKA_HOST = "gateway.akka.host"
  final val CONFIG_AKKA_PORT = "gateway.akka.port"
  final val CONFIG_PARTITION_LIST = "partition.list"

  case class PartitionOnline(partition: ActorRef)

  case class PartitionOffline(partition: ActorRef)
}

class Region(appConfig: Properties, coordinator: Coordinator) extends Actor {

  val log = Logging.getLogger(context.system, this)

  import Region._

  val akkaPort = appConfig.getProperty(CONFIG_AKKA_PORT, "2552").toInt
  val akkaAddress = appConfig.getProperty(CONFIG_AKKA_HOST, null) match {
    case null => s"akka://${context.system.name}"
    case host => s"akka.tcp://${context.system.name}@${host}:${akkaPort}"
  }

  val partitionList = appConfig.getProperty(CONFIG_PARTITION_LIST).split("\\,").map(_.toInt).toList

  private var partitions = scala.collection.mutable.Map[ActorRef, String]()

  override def preStart(): Unit = {
    //initiate creation of partitions and register them once booted
    for (p <- partitionList) {
      context.actorOf(Props(new Partition(p)), name = System.nanoTime() + "-" + p)
    }
  }

  override def postStop(): Unit = {
    partitions.foreach { case (ref, handle) =>
      log.info(s"Unregistering partition: handle=${partitions(ref)}, path=${ref.path}")
      coordinator.unregister(partitions(ref))
    }
  }

  override def receive: Receive = {
    case PartitionOnline(ref) =>
      val partitionActorPath = ActorPath.fromString(s"${akkaAddress}${ref.path.toStringWithoutAddress}")
      val handle = coordinator.register(partitionActorPath)
      log.info(s"Partition online: handle=$handle, path=${partitionActorPath}")
      partitions += (ref -> handle)
//      context.watch(ref)

    case PartitionOffline(ref) =>
      log.info(s"Partition offline: handle=${partitions(ref)}, path=${ref.path}")
      coordinator.unregister(partitions(ref))
//      context.unwatch(ref)

  }
}


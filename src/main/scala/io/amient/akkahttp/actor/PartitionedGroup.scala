package io.amient.akkahttp.actor

import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorSystem
import akka.dispatch.Dispatchers
import akka.event.Logging
import akka.routing._
import io.amient.akkahttp.actor.Partition.Keyed

import scala.collection.immutable

final case class PartitionedGroup(numPartitions: Int) extends Group {

  override def paths(system: ActorSystem) = List()

  override val routerDispatcher: String = Dispatchers.DefaultDispatcherId

  override def createRouter(system: ActorSystem): Router = {
    val log = Logging.getLogger(system, this)
    log.info("Creating PartitionedGroup router")

    val defaultLogic = RoundRobinRoutingLogic()

    val partitioningLogic = new RoutingLogic {

      private var prevRoutees: immutable.IndexedSeq[Routee] = immutable.IndexedSeq()
      private val currentRouteMap = new ConcurrentHashMap[Int, Routee]()

      def abs(i: Int): Int = math.abs(i) match {
        case Int.MinValue => 0
        case a => a
      }

      def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {

        if (!prevRoutees.eq(routees)) {
          prevRoutees.synchronized {
            if (!prevRoutees.eq(routees)) {
              currentRouteMap.clear()
              routees.foreach {
                case actorRefRoutee: ActorRefRoutee =>
                  currentRouteMap.put(actorRefRoutee.ref.path.name.substring(10).toInt, actorRefRoutee)
              }
              prevRoutees = routees
            }
          }
        }
        val partition = abs(message.hashCode()) % numPartitions

        //TODO test the suspended scenario
        if (!currentRouteMap.containsKey(partition)) throw new IllegalStateException(
          s"Partition `$partition` is not represented by any Actor - " +
            s"this shouldn't happen - gateway should suspend all requests until all partitions are present")

        currentRouteMap.get(partition)
      }

    }

    new Router(new RoutingLogic {
      def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
        message match {
          case k: Keyed[_] => partitioningLogic.select(message, routees)
          case _ => defaultLogic.select(message, routees)
        }
      }
    })
  }

}
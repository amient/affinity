package io.amient.akkahttp.actor

import java.util.Properties

import akka.actor.{ActorPath, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import io.amient.akkahttp.TestCoordinator
import io.amient.akkahttp.actor.Partition.SimulateError
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._


class RegionSpec() extends TestKit(ActorSystem("MySpec")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A Region Actor" must {
    "must keep Coordinator Updated during partition failure & restart scenario" in {
      val partitions = scala.collection.mutable.Set[String]()
      val coordinator = new TestCoordinator(partitions)
      val props = new Properties()
      val d  = 1 second
      implicit val timeout = Timeout(d)

      //wait for a region of 4 partitions to be online
      props.put(Region.CONFIG_PARTITION_LIST, "0,1,2,3")
      system.actorOf(Props(new Region(props, coordinator)), name = "region")
      awaitCond(partitions.size == 4)

      //first stop Partition explicitly - it shouldn't be restarted
      import system.dispatcher
      system.actorSelection(ActorPath.fromString(partitions.head)).resolveOne() onSuccess {
        case actorRef => system.stop(actorRef)
      }
      awaitCond(partitions.size == 3)

      //now simulate error in one of the partitions
      val partitionToFail = partitions.head
      system.actorSelection(ActorPath.fromString(partitions.head)).resolveOne() onSuccess{
        case actorRef => actorRef ! SimulateError(new IllegalStateException)
      }
      awaitCond(partitions.size == 2 && !partitions.contains(partitionToFail))
      //it had a failure, it should be restarted
      awaitCond(partitions.size == 3)

    }
  }

}

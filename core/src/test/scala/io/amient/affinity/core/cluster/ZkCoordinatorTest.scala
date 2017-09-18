package io.amient.affinity.core.cluster

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{NotFound, OK}
import akka.util.Timeout
import com.typesafe.config.ConfigValueFactory
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.{GatewayHttp}
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.testutil.{MyTestPartition, SystemTestBaseWithZk}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.collection.JavaConversions._

class ZkCoordinatorTest extends FlatSpec with SystemTestBaseWithZk with Matchers {

  def config = configure("distributedit")

  val system = ActorSystem.create("test", config)
  val gatewayNode = new TestGatewayNode(config, new GatewayHttp {

    val regionService = service("region")

    //FIXME
//    import MyTestPartition._
//    import context.dispatcher
//
//    implicit val scheduler = context.system.scheduler
//
//    override def handle: Receive = {
//      case HTTP(GET, PATH(key), _, response) =>
//        implicit val timeout = Timeout(500 milliseconds)
//        delegateAndHandleErrors(response, keyspace1 ack GetValue(key)) {
//          _ match {
//            case None => HttpResponse(NotFound)
//            case Some(value) => Encoder.json(OK, value, gzip = false)
//          }
//        }
//    }
  })

  import gatewayNode._

  val regionNode1 = new Node(config.withValue("affinity.node.container.region",
    ConfigValueFactory.fromIterable(List(0,1,2,3))))

//  val region2 = new Node(config.withValue("affinity.node.container.region",
//    ConfigValueFactory.fromIterable(List(1,3,5,7))))

  gatewayNode.awaitClusterReady {
    regionNode1.start()
//    region2.start()
  }


  override def afterAll(): Unit = {
    try {
      gatewayNode.shutdown()
//      region2.shutdown()
      regionNode1.shutdown()
    } finally {
      super.afterAll()
    }
  }

  //  try {
  //    val coordinator1 = Coordinator.create(system, "group1")
  //    val actor1 = system.actorOf(Props(new Actor {
  //      override def receive: Receive = {
  //        case null =>
  //      }
  //    }), "actor1")
  //    coordinator1.register(actor1.path)
  //    val update1 = new AtomicReference[String]("")
  //    update1 synchronized {
  //      coordinator1.watch(system.actorOf(Props(new Actor {
  //        override def receive: Receive = {
  //          case MasterStatusUpdate(g, add, del) => update1 synchronized update1.set(s"$g, ${add.size}, ${del.size}")
  //        }
  //      }), "subscriber1"), false)
  //    }
  //    coordinator1.close()
  //
  //    val coordinator2 = Coordinator.create(system, "group1")
  //    val update2 = new AtomicReference[String]("")
  //    update2 synchronized {
  //      coordinator2.watch(system.actorOf(Props(new Actor {
  //        override def receive: Receive = {
  //          case MasterStatusUpdate(g, add, del) => update2 synchronized update2.set(s"$g, ${add.size}, ${del.size}")
  //        }
  //      }), "subscriber2"), true)
  //      update2.wait(1000)
  //      update2.get should be("group1, 1, 0")
  //      update1.get should be("group1, 1, 0")
  //    }
  //    coordinator2.close()
  //
  //
  //  } finally {
  //    system.terminate()
  //  }


}

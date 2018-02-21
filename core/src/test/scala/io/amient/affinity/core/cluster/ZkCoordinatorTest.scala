package io.amient.affinity.core.cluster

import akka.actor.ActorSystem
import com.typesafe.config.ConfigValueFactory
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.core.actor.{GatewayHttp, Routed}
import io.amient.affinity.core.util.{Reply, AffinityTestBase}
import io.amient.affinity.kafka.EmbeddedZooKeeper
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._

case class ZkTestKey(val id: Int) extends AvroRecord with Routed with Reply[Option[ZkTestValue]] {
  override def key = this
}

case class ZkTestValue(items: List[Int]) extends AvroRecord

class ZkCoordinatorTest extends FlatSpec with AffinityTestBase with EmbeddedZooKeeper with Matchers {

  def config = configure("distributedit", zkConnect = Some(zkConnect))

  val system = ActorSystem.create("test", config)
  val gatewayNode = new TestGatewayNode(config, new GatewayHttp {

    val regionService = keyspace("region")

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

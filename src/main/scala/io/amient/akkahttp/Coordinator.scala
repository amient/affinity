package io.amient.akkahttp

import java.util
import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ActorPath, ActorRef, ActorSystem}
import akka.routing.{ActorRefRoutee, AddRoutee}
import akka.util.Timeout
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class Coordinator(val zkRoot: String, zkConnect: String, zkSessionTimeout: Int, zkConnectTimeout: Int)

  extends ZkClient(zkConnect, zkSessionTimeout, zkConnectTimeout, new ZkSerializer {
    def serialize(o: Object): Array[Byte] = o.toString.getBytes

    override def deserialize(bytes: Array[Byte]): Object = new String(bytes)
  }) {
  def addAnchor(anchor: ActorPath) = {
    val anchorZkNode = s"${zkRoot}/${anchor.hashCode()}"
    create(anchorZkNode, anchor.toString(), CreateMode.EPHEMERAL_SEQUENTIAL)
  }


  if (!exists(zkRoot)) {
    println("creating zk root " + zkRoot)
    createPersistent(zkRoot, true)
  }

  private val current = new ConcurrentHashMap[ActorPath, Boolean]()

  def subscribeToPartitions(system: ActorSystem, gateway: ActorRef): Unit = {

    import system.dispatcher

    def listAsIndexedSeq(list: util.List[String]) = list.asScala.toIndexedSeq

    def addAnchor(anchor: String): Unit = {
      implicit val timeout = new Timeout(24 hours)
//      System.err.println(s"looking up $anchor")
      system.actorSelection(anchor).resolveOne().onSuccess {
        case partitionActorRef =>
//          System.err.println(partitionActorRef.path)
          current.put(partitionActorRef.path, true)
          gateway ! AddRoutee(ActorRefRoutee(partitionActorRef))
      }
    }

//    subscribeChildChanges(zkRoot, new IZkChildListener() {
//      override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
//        val newList = listAsIndexedSeq(currentChilds)
//        newList.foreach { newPath =>
//          if (!current.containsKey(newPath)) {
//            current.put(newPath, null)
//
//            actor ! AddRoutee(ActorRefRoutee())
//          }
//        }
//      }
//    })
//
    listAsIndexedSeq(getChildren(zkRoot)).foreach { seqId =>
      val anchorNode = s"${zkRoot}/$seqId"
      addAnchor(readData(anchorNode))
    }
  }

}

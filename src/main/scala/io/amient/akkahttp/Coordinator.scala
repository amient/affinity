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
import scala.util.Success

class Coordinator(val zkRoot: String, zkConnect: String, zkSessionTimeout: Int, zkConnectTimeout: Int)

  extends ZkClient(zkConnect, zkSessionTimeout, zkConnectTimeout, new ZkSerializer {
    def serialize(o: Object): Array[Byte] = o.toString.getBytes

    override def deserialize(bytes: Array[Byte]): Object = new String(bytes)
  }) {

  def addAnchor(anchor: ActorPath) = {
    val anchorZkNode = s"${zkRoot}/${anchor.hashCode()}"
    create(anchorZkNode, anchor.toString(), CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  def removeAnchor(anchor: ActorPath) = {
    println("removing " + anchor)
  }

  if (!exists(zkRoot)) {
    println("creating zk root " + zkRoot)
    createPersistent(zkRoot, true)
  }

  private val current = new ConcurrentHashMap[ActorPath, String]()

  def subscribeToPartitions(system: ActorSystem, gateway: ActorRef): Unit = {

    import system.dispatcher

    def listAsIndexedSeq(list: util.List[String]) = list.asScala.toIndexedSeq

    def addAnchor(seqId: String): Unit = {
      val anchorNode = s"${zkRoot}/$seqId"
      val anchor:String = readData(anchorNode)
      implicit val timeout = new Timeout(24 hours)
      system.actorSelection(anchor).resolveOne() andThen {
        case Success(partitionActorRef) =>
          if (!current.containsKey(partitionActorRef.path)) {
            current.put(partitionActorRef.path, seqId)
            gateway ! AddRoutee(ActorRefRoutee(partitionActorRef))
          }

      }
    }

    subscribeChildChanges(zkRoot, new IZkChildListener() {
      override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
        val newList = listAsIndexedSeq(currentChilds)
        newList.foreach(addAnchor(_))
        //TODO delete missing
        newList.foreach(println)
        println("-----------------")

      }
    })

    listAsIndexedSeq(getChildren(zkRoot)).foreach(addAnchor(_))

  }

}

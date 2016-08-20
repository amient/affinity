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

  /**
    * @param actorPath of the actor that needs to managed as part of coordinated group
    * @return unique coordinator handle which points to the registered ActorPath
    */
  def register(actorPath: ActorPath): String = {
    create(s"${zkRoot}/", actorPath.toString(), CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  def unregister(handle: String) = {
    delete(handle)
  }

  if (!exists(zkRoot)) {
    createPersistent(zkRoot, true)
  }

  private val current = new ConcurrentHashMap[ActorPath, String]()

  def listenToPartitionAssignemnts(system: ActorSystem, gateway: ActorRef): Unit = {

    import system.dispatcher

    def listAsIndexedSeq(list: util.List[String]) = list.asScala.toIndexedSeq

    def addAnchor(seqId: String): Unit = {
      val anchorPath = s"${zkRoot}/$seqId"
      val anchor: String = readData(anchorPath)
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
        if (currentChilds != null) {
          val newList = listAsIndexedSeq(currentChilds)
          newList.foreach(addAnchor(_))
          current.entrySet().asScala.foreach { entry =>
            if (!newList.contains(entry.getValue)) {
              current.remove(entry.getKey)
            }
          }
        }
      }
    })

    listAsIndexedSeq(getChildren(zkRoot)).foreach(addAnchor(_))

  }

}

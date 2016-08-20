package io.amient.akkahttp

import java.util
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ActorPath, ActorRef, ActorSystem}
import akka.routing.{ActorRefRoutee, AddRoutee, RemoveRoutee}
import akka.util.Timeout
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Success

object Coordinator {

  final val CONFIG_IMPLEMENTATION = "coordinator.implementation"

  def fromProperties(appConfig: Properties): Coordinator = {
    appConfig.getProperty(CONFIG_IMPLEMENTATION, "zookeeper") match {
      case "zookeeper" => new ZkCoordinator(appConfig)
    }
  }
}

trait Coordinator {
  /**
    * @param actorPath of the actor that needs to managed as part of coordinated group
    * @return unique coordinator handle which points to the registered ActorPath
    */
  def register(actorPath: ActorPath): String;

  /**
    * unregister previously registered ActorPath
    *
    * @param handle handler returned from the prior register() method call
    * @return
    */
  def unregister(handle: String)

  /**
    * watch changes in the coordinate group of routees. The implementation must
    * call addRouteeActor() and removeRouteeActor() methods when respective
    * changes occur.
    *
    * @param system ActorSystem instance
    * @param watcher actor which will receive the messages
    */
  def watchRoutees(system: ActorSystem, watcher: ActorRef): Unit

  def close(): Unit

  /**
    * internal data structure
    */
  private val handles = new ConcurrentHashMap[ActorPath, (String, ActorRef)]()

  def getAllHandles: Set[String] = handles.entrySet().asScala.map(_.getValue._1).toSet

  protected def removeRoutee(watcher: ActorRef, routeeHandle: String): Unit = {
    handles.entrySet().asScala.foreach { case entry =>
      val (handle, actorRef) = entry.getValue
      if (routeeHandle == handle) {
        watcher ! RemoveRoutee(ActorRefRoutee(actorRef))
        handles.remove(entry.getKey)
      }
    }
  }

  protected def addRouteeActor(system: ActorSystem,
                               watcher: ActorRef,
                               routeeHandle: String,
                               routeePath: String): Unit = {
    import system.dispatcher
    implicit val timeout = new Timeout(24 hours)
    system.actorSelection(routeePath).resolveOne() andThen {
      case Success(partitionActorRef) =>
        if (!handles.containsKey(partitionActorRef.path)) {
          handles.put(partitionActorRef.path, (routeeHandle, partitionActorRef))
          watcher ! AddRoutee(ActorRefRoutee(partitionActorRef))
        }
    }
  }


}

object ZkCoordinator {
  final val CONFIG_ZOOKEEPER_CONNECT = "coordinator.zookeeper.connect"
  final val CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS = "coordinator.zookeeper.connect.timeout.ms"
  final val CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS = "coordinator.zookeeper.session.timeout.ms"
  final val CONFIG_ZOOKEEPER_ROOT = "coordinator.zookeeper.root"
}

class ZkCoordinator(appConfig: Properties) extends Coordinator {

  import ZkCoordinator._

  val zkConnect = appConfig.getProperty(CONFIG_ZOOKEEPER_CONNECT, "localhost:2181")
  val zkConnectTimeout = appConfig.getProperty(CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS, "6000").toInt
  val zkSessionTimeout = appConfig.getProperty(CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS, "6000").toInt
  val zkRoot = appConfig.getProperty(CONFIG_ZOOKEEPER_ROOT, "zookeeper")

  private val zk = new ZkClient(
    zkConnect, zkSessionTimeout, zkConnectTimeout, new ZkSerializer {
      def serialize(o: Object): Array[Byte] = o.toString.getBytes

      override def deserialize(bytes: Array[Byte]): Object = new String(bytes)
    })

  if (!zk.exists(zkRoot)) {
    zk.createPersistent(zkRoot, true)
  }

  override def register(actorPath: ActorPath): String = {
    zk.create(s"${zkRoot}/", actorPath.toString(), CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  override def unregister(handle: String) = zk.delete(handle)

  override def close(): Unit = zk.close()

  override def watchRoutees(system: ActorSystem, watcher: ActorRef): Unit = {

    def listAsIndexedSeq(list: util.List[String]) = list.asScala.toIndexedSeq


    def addRoutee(handle: String): Unit = {
      val keyActorPath: String = zk.readData(s"${zkRoot}/$handle")
      addRouteeActor(system, watcher, handle, keyActorPath)
    }

    zk.subscribeChildChanges(zkRoot, new IZkChildListener() {
      override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
        if (currentChilds != null) {
          val newList = listAsIndexedSeq(currentChilds)
          newList.foreach(addRoutee(_))
          getAllHandles.filter(!newList.contains(_)).foreach(handle => removeRoutee(watcher, handle))
        }
      }
    })

    listAsIndexedSeq(zk.getChildren(zkRoot)).foreach(addRoutee(_))

  }

}

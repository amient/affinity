package io.amient.affinity.example.symmetric

import java.util.Properties

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.HttpInterface
import io.amient.affinity.core.actor.Controller.{CreateGateway, CreateRegion}
import io.amient.affinity.core.actor.{Controller, Region}
import io.amient.affinity.core.cluster.{Cluster, Coordinator, ZkCoordinator}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

object SymmetricClusterNode extends App {

  final val ActorSystemName = "MySymmetricCluster"

  override def main(args: Array[String]): Unit = {

    try {
      require(args.length >=5)

      val akkaPort = args(0).toInt
      val host = args(1)
      val httpPort = args(2).toInt
      val numPartitions = args(3).toInt
      val partitionList = args(4) // coma-separated list of partitions assigned to this node
      val zkConnect = if (args.length > 5) args(5) else "localhost:2181"

      val zkSessionTimeout = 6000
      val zkConnectTimeout = 30000
      val zkRoot = "/akka"

      println(s"Http: $host:$httpPort")
      println(s"Akka: $host:$akkaPort")
      println(s"Zookeeper: $zkConnect")
      println(s"Partitions: $partitionList of $numPartitions")

      val appConfig = new Properties()
      appConfig.put(HttpInterface.CONFIG_HTTP_HOST, host)
      appConfig.put(HttpInterface.CONFIG_HTTP_PORT, httpPort.toString)
      appConfig.put(Cluster.CONFIG_NUM_PARTITIONS, numPartitions.toString)
      appConfig.put(Region.CONFIG_AKKA_HOST, host)
      appConfig.put(Region.CONFIG_AKKA_PORT, akkaPort.toString)
      appConfig.put(Region.CONFIG_PARTITION_LIST, partitionList)
      appConfig.put(Coordinator.CONFIG_COORDINATOR_CLASS, classOf[ZkCoordinator].getName)
      appConfig.put(ZkCoordinator.CONFIG_ZOOKEEPER_CONNECT, zkConnect)
      appConfig.put(ZkCoordinator.CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS, zkConnectTimeout.toString)
      appConfig.put(ZkCoordinator.CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS, zkSessionTimeout.toString)
      appConfig.put(ZkCoordinator.CONFIG_ZOOKEEPER_ROOT,zkRoot)

      val systemConfig = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$akkaPort")
        .withFallback(ConfigFactory.load("application"))

      implicit val system = ActorSystem(ActorSystemName, systemConfig)

      val controller = system.actorOf(Props(new Controller(appConfig)), name = "controller")

      //this cluster is symmetric - all nodes serve both as Gateways and Regions
      controller ! CreateGateway(classOf[RequestHandler])
      controller ! CreateRegion(Props(new LocalHandler))

      //in case the process is stopped from outside
      sys.addShutdownHook {
        system.terminate()
        //we cannot use the future returned by system.terminate() because shutdown may have already been invoked
        Await.ready(system.whenTerminated, 30 seconds) // TODO shutdown timeout by configuration
      }

    } catch {
      case e: IllegalArgumentException =>
        e.printStackTrace()
        System.exit(1)
      case NonFatal(e) =>
        e.printStackTrace()
        System.exit(2)
      case e: Throwable =>
        e.printStackTrace()
        System.exit(3)
    }
  }
}


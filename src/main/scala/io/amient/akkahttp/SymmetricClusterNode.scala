package io.amient.akkahttp

import java.util.Properties

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import io.amient.akkahttp.actor.{Gateway, HttpInterface}

import scala.concurrent.Await
import scala.concurrent.duration._

object SymmetricClusterNode extends App {

  final val ActorSystemName = "MySymmetricCluster"

  override def main(args: Array[String]): Unit = {

    try {
      require(args.length >= 5)

      val akkaPort = args(0).toInt
      val host = args(1)
      val httpPort = args(2).toInt
      val numPartitions = args(3).toInt
      val partitionList = args(4) // coma-separated list of partitions assigned to this node
      val zkConnect = if (args.length > 5) args(5) else "localhost:2181"
      val zkSessionTimeout = 6000
      val zkConnectTimeout = 30000
      val zkRoot = "/akka"

      println(s"Http port: $httpPort")
      println(s"Akka port: $akkaPort")
      println(s"Partitions: $partitionList of $numPartitions")

      val systemConfig = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$akkaPort")
        .withFallback(ConfigFactory.load("application"))

      val appConfig = new Properties()
      appConfig.put(HttpInterface.CONFIG_HTTP_HOST, host)
      appConfig.put(HttpInterface.CONFIG_HTTP_PORT, httpPort.toString)
      appConfig.put(Gateway.CONFIG_AKKA_HOST, host)
      appConfig.put(Gateway.CONFIG_AKKA_PORT, akkaPort.toString)
      appConfig.put(Gateway.CONFIG_NUM_PARTITIONS, numPartitions.toString)
      appConfig.put(Gateway.CONFIG_PARTITION_LIST, partitionList)
      appConfig.put(Coordinator.CONFIG_IMPLEMENTATION,"zookeeper")
      appConfig.put(ZkCoordinator.CONFIG_ZOOKEEPER_CONNECT, zkConnect)
      appConfig.put(ZkCoordinator.CONFIG_ZOOKEEPER_CONNECT_TIMEOUT_MS, zkConnectTimeout.toString)
      appConfig.put(ZkCoordinator.CONFIG_ZOOKEEPER_SESSION_TIMEOUT_MS, zkSessionTimeout.toString)
      appConfig.put(ZkCoordinator.CONFIG_ZOOKEEPER_ROOT,zkRoot)

      implicit val system = ActorSystem(ActorSystemName, systemConfig)

      val httpInterface = system.actorOf(Props(new HttpInterface(appConfig)), name = "interface")

      sys.addShutdownHook {
        Await.result(system.terminate(), 30 seconds)
      }
    } catch {
      case e: Throwable =>
        System.err.println("Error during Node Startup")
        //e.printStackTrace()
        System.err.println(e.getMessage)
        System.exit(1)
    }
  }
}


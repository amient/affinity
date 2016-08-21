package io.amient.akkahttp

import java.util.Properties

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import io.amient.akkahttp.actor.{Gateway, HttpInterface}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

object SymmetricClusterNode extends App {

  final val ActorSystemName = "MySymmetricCluster"

  override def main(args: Array[String]): Unit = {

    try {
      require(args.length == 6)

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
      println(s"Zookeeper: $zkConnect")
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

      //TODO is this how child actors should be created - seems unsafe
      val gateway = system.actorOf(Props(new Gateway(appConfig)), name = "gateway")

      val httpInterface = new HttpInterface(appConfig, gateway)

      sys.addShutdownHook {
        try {
          Await.result(system.terminate(), 30 seconds)
        } finally {
          httpInterface.close()
        }
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


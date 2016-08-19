package io.amient.akkahttp

import akka.actor.{ActorPath, ActorSystem, Props}
import akka.event.Logging
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.amient.akkahttp.actor.{Gateway, HttpInterface, Partition}

import scala.concurrent.Await
import scala.concurrent.duration._

object SymmetricClusterNode extends App {

  final val ActorSystemName = "MySymmetricCluster"

  val akkaPort = args(0).toInt
  val host = args(1)
  val httpPort = args(2).toInt
  val numPartitions = args(3).toInt
  val partitionList = args(4).split("\\,").map(_.toInt).toList
  val zkConnect = if (args.length > 5) args(5) else "localhost:2181"
  val zkSessionTimeout = 6000
  val zkConnectTimeout = 30000
  val zkRoot = "/akka"


  println(s"Http port: $httpPort")
  println(s"Akka port: $akkaPort")
  println(s"Partitions: $partitionList of $numPartitions")
  println(s"Zookeeper: $zkConnect")


  val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$akkaPort")
    .withFallback(ConfigFactory.load("application"))

  implicit val system = ActorSystem(ActorSystemName, config)

  val log = Logging.getLogger(system, this)

  val coordinator = new Coordinator(zkRoot, zkConnect, zkSessionTimeout, zkConnectTimeout)

  val gateway = system.actorOf(Props(
    new Gateway(host, akkaPort, numPartitions, partitionList, coordinator)), name = "gateway")

  val httpInterface = system.actorOf(Props(
    new HttpInterface(host, httpPort, gateway)), name = "interface")

  sys.addShutdownHook {
    Await.result(system.terminate(), 30 seconds)
    coordinator.close()
  }
}


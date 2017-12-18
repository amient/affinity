package io.amient.affinity.core.actor

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{ActorRef, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.routing._
import akka.serialization.SerializationExtension
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller.{CreateGateway, GracefulShutdown}
import io.amient.affinity.core.actor.Keyspace.{CheckServiceAvailability, ServiceAvailability}
import io.amient.affinity.core.cluster.Coordinator.MasterStatusUpdate
import io.amient.affinity.core.cluster.{Coordinator, Node}
import io.amient.affinity.core.config.CfgStruct
import io.amient.affinity.core.serde.avro.AvroSerdeProxy
import io.amient.affinity.core.storage.State

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

object Gateway {

  class GatewayConf extends CfgStruct[GatewayConf] {
    val Class = cls("class", classOf[Gateway], false)
    val SuspendQueueMaxSize = integer("suspend.queue.max.size", 1000)
    val Http = struct("http", new GatewayHttp.HttpConf, false)
  }

  final case class GatewayClusterStatus(suspended: Boolean)
}

trait Gateway extends ActorHandler with ActorState {

  private val log = Logging.getLogger(context.system, this)

  import Gateway._

  private val conf = Node.Conf(context.system.settings.config).Affi

  if (conf.Gateway.Http.isDefined && !classOf[GatewayHttp].isAssignableFrom(this.getClass)) {
    log.warning("affinity.gateway.http interface is configured but the node is trying " +
      "to instantiate a non-http gateway. This may lead to uncertainity in the Controller.")
  }

  private implicit val scheduler = context.system.scheduler

  import context.dispatcher

  def onClusterStatus(suspended: Boolean): Unit = ()

  private val keyspaces = mutable.Map[String, (Coordinator, ActorRef, AtomicBoolean)]()
  private val broadcasts = mutable.Map[String, (State[_, _], AtomicBoolean)]()
  private var handlingSuspended = true

  def broadcast[K: ClassTag, V: ClassTag](broadcastName: String): State[K, V] = {
    broadcasts.get(broadcastName) match {
      case Some((broadcastState, _)) => broadcastState.asInstanceOf[State[K, V]]
      case None =>
        val bc = state[K,V](broadcastName, conf.Broadcast(broadcastName))
        broadcasts += (broadcastName -> (bc, new AtomicBoolean(true)))
        bc
    }
  }

  def keyspace(group: String): ActorRef = {
    keyspaces.get(group) match {
      case Some((_, keyspaceActor, _)) => keyspaceActor
      case None =>
        if (!handlingSuspended) throw new IllegalStateException("All required affinity services must be declared in the constructor")
        val serviceConf = conf.Keyspace(group)
        val ks = context.actorOf(Props(new Keyspace(serviceConf.config())), name = group)
        val coordinator = Coordinator.create(context.system, group)
        context.watch(ks)
        keyspaces += (group -> (coordinator, ks, new AtomicBoolean(true)))
        ks
    }
  }

  private def checkClusterStatus(msg: Option[ServiceAvailability] = None): Unit = {
    val gatewayShouldBeSuspended = keyspaces.exists(_._2._3.get)
    if (gatewayShouldBeSuspended != handlingSuspended) {
      handlingSuspended = gatewayShouldBeSuspended
      onClusterStatus(gatewayShouldBeSuspended)
      if (self.path.name == "gateway") {
        msg.foreach(context.system.eventStream.publish(_)) // this one is for IntegrationTestBase
        context.system.eventStream.publish(GatewayClusterStatus(gatewayShouldBeSuspended)) //this one for SystemTestBase
      }
    }
  }

  abstract override def preStart(): Unit = {
    super.preStart()
    checkClusterStatus()
    tailState() // any state store registered in the gateway layer is broadcast, so all are tailing
    keyspaces.foreach {
      case(_, (coordinator, _, _)) => coordinator.watch(self, global = true)
    }
  }

  abstract override def postStop(): Unit = {
    super.postStop()
    keyspaces.values.foreach(_._1.unwatch(self))
  }

  abstract override def manage = super.manage orElse {

    case msg@CreateGateway if !classOf[GatewayHttp].isAssignableFrom(this.getClass) =>
      context.parent ! Controller.GatewayCreated(-1)

    case msg@MasterStatusUpdate(group, add, remove) => sender.reply(msg) {
      val service: ActorRef = keyspaces(group)._2
      remove.foreach(ref => service ! RemoveRoutee(ActorRefRoutee(ref)))
      add.foreach(ref => service ! AddRoutee(ActorRefRoutee(ref)))
      service ! CheckServiceAvailability(group)
    }

    case msg@ServiceAvailability(group, suspended) =>
      val (_, _, currentlySuspended) = keyspaces(group)
      if (currentlySuspended.get != suspended) {
        keyspaces(group)._3.set(suspended)
        checkClusterStatus(Some(msg))
      }

    case request@GracefulShutdown() => sender.reply(request) {
      context.stop(self)
    }

  }

  def describeAvro: scala.collection.immutable.Map[String, String] = {
    val serde = SerializationExtension(context.system).serializerByIdentity(200).asInstanceOf[AvroSerdeProxy].internal
    serde.describeSchemas.map {
      case (id, schema) => ((id.toString, schema.toString))
    }
  }

  def describeServices: scala.collection.immutable.Map[String, String] = {
    keyspaces.toMap.map { case (group, (_, actorRef, _)) => (group, actorRef.path.toString) }
  }

  def describeRegions: scala.collection.immutable.List[String] = {
    val t = 60 seconds
    implicit val timeout = Timeout(t)
    val routeeSets = Future.sequence {
      keyspaces.toMap.map { case (group, (_, actorRef, _)) =>
        actorRef ? GetRoutees map (_.asInstanceOf[Routees])
      }
    }
    Await.result(routeeSets, t).map(_.routees).flatten.map(_.toString).toList.sorted
  }

}

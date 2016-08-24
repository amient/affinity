package io.amient.akkahttp

import akka.actor.{ActorPath, ActorRef, ActorSystem}

class TestCoordinator(storage: scala.collection.mutable.Set[String]) extends Coordinator {

//  @volatile private var watcher: ActorRef = null
  override def watchRoutees(system: ActorSystem, watcher: ActorRef): Unit = {
//    this.watcher = watcher
    ???
  }

  override def register(actorPath: ActorPath): String = {
    val handle = actorPath.toString
    storage.add(handle)
    handle
  }

  override def unregister(handle: String): Unit = {
    storage.remove(handle)
  }

  override def close(): Unit = {}
}

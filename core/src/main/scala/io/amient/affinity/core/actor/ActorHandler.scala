package io.amient.affinity.core.actor

import akka.actor.Actor

trait ActorHandler extends Actor {

  final override def receive: Receive = manage orElse handle orElse unhandled

  def manage: Receive = PartialFunction.empty

  def handle: Receive = PartialFunction.empty

  def unhandled: Receive = PartialFunction.empty

}

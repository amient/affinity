package io.amient.affinity.core

import akka.actor.Actor.Receive
import org.slf4j.LoggerFactory


object StackableSpec extends App {

  val gate = new Gate with Module1 with Module2
  gate.handle.apply("HI")
  gate.handle.apply(100)
}

abstract class Gate {
  def handle: Receive = {
    case null =>
  }
}

trait Module1 extends Gate {

  val sharedData = "shared-data"

  private val log = LoggerFactory.getLogger(classOf[Module1])

  abstract override def handle: Receive = super.handle orElse {
    case in: Int => log.info(s"Handled by Module1: " + in)
  }
}

trait Module2 extends Gate {
  self: Module1 => //this module requries another module

  private val log = LoggerFactory.getLogger(classOf[Module2])

  abstract override def handle: Receive = super.handle orElse {
    case in: String => log.info(s"Handled by Module2: " + in +s" using $sharedData")
  }
}








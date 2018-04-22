package io.amient.affinity.core.cluster

import io.amient.affinity.core.actor.{Partition, Routed}
import io.amient.affinity.core.util.Reply

class FailoverTestPartition(store: String) extends Partition {

  import FailoverTestPartition._
  import context.dispatcher

  val data = state[String, String](store)

  override def handle: Receive = {
    case request@GetValue(key) => request(sender) ! data(key)
    case request@PutValue(key, value) => request(sender) ! data.replace(key, value)
  }
}


object FailoverTestPartition {

  case class GetValue(key: String) extends Routed with Reply[Option[String]] {
    override def hashCode(): Int = key.hashCode
  }

  case class PutValue(key: String, value: String) extends Routed with Reply[Option[String]] {
    override def hashCode(): Int = key.hashCode
  }

}

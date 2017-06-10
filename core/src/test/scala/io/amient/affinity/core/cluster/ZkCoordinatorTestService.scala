package io.amient.affinity.core.cluster

import io.amient.affinity.core.actor.Partition
import io.amient.affinity.core.transaction.{TestKey, TestValue}
import io.amient.affinity.core.ack

class ZkCoordinatorTestService extends Partition {

  override def handle: Receive = {
    case request@TestKey(id) => sender.reply(request){
      Some(TestValue(List(id)))
    }
  }
}

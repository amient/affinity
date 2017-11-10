package io.amient.affinity.avro.util

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer

class ZooKeeperClient(zkConnect: String, zkSessionTimeout: Int = 30000, zkConnectTimeout: Int = 6000)
  extends ZkClient(zkConnect, zkSessionTimeout, zkConnectTimeout, new ZkSerializer {
    def serialize(o: Object): Array[Byte] = o.toString.getBytes

    override def deserialize(bytes: Array[Byte]): Object = new String(bytes)
  })

package io.amient.affinity.core.util

import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.storage.Storage.StorageConf
import io.amient.affinity.stream.{BinaryRecord, BinaryStream}

import scala.collection.JavaConversions._

class OutputDataStream[K, V](keySerde: AbstractSerde[_ >: K], valSerde: AbstractSerde[_ >: V], streamConf: StorageConf) {

  lazy val stream = BinaryStream.bindNewInstance(streamConf)

  def output(data: Iterator[(K, V)]): Unit = {
    stream.publish(data.map {
      case (k, v) if v.isInstanceOf[EventTime] => new BinaryRecord(keySerde.toBytes(k), valSerde.toBytes(v), v.asInstanceOf[EventTime].eventTimeUnix)
      case (k, v) => new BinaryRecord(keySerde.toBytes(k), valSerde.toBytes(v), EventTime.unix)
    })
  }

  def flush() = stream.flush()

  def close() = {
    try flush() finally try stream.close() finally {
      keySerde.close()
      valSerde.close()
    }
  }
}

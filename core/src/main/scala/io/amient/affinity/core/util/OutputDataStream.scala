package io.amient.affinity.core.util

import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.storage.{LogStorage, LogStorageConf, Record}

class OutputDataStream[K, V](keySerde: AbstractSerde[_ >: K], valSerde: AbstractSerde[_ >: V], conf: LogStorageConf) {

  lazy val storage = LogStorage.newInstance(conf)

  def write(data: Iterator[(K, V)]): Unit = {
    val records = data.map {
      case (k, v) if v.isInstanceOf[EventTime] => new Record(keySerde.toBytes(k), valSerde.toBytes(v), v.asInstanceOf[EventTime].eventTimeUnix)
      case (k, v) => new Record(keySerde.toBytes(k), valSerde.toBytes(v), EventTime.unix)
    }
    records.foreach(storage.append)
  }

  def flush() = storage.flush()

  def close() = {
    try flush() finally try storage.close() finally {
      keySerde.close()
      valSerde.close()
    }
  }
}

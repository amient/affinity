package io.amient.affinity.avro.util

import java.util
import java.util.concurrent.ConcurrentHashMap

class ThreadLocalCache[K, I] extends ThreadLocal[java.util.HashMap[K, I]] {

  val global = new ConcurrentHashMap[K, I]()

  override def initialValue() = new util.HashMap[K, I]()

  def getOrInitialize(key: K, initializer: => I) = {
    get().get(key) match {
      case null =>
        val g = global.get(key) match {
          case null =>
            val init = initializer
            global.put(key, init)
            init
          case some => some
        }
        get().put(key, g)
      case some => some
    }
  }
}

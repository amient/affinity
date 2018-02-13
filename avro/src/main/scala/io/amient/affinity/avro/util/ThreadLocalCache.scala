package io.amient.affinity.avro.util

import java.util
import java.util.concurrent.ConcurrentHashMap

class ThreadLocalCache[K, V] extends ThreadLocal[java.util.HashMap[K, V]] {

  val global = new ConcurrentHashMap[K, V]()

  override def initialValue() = new util.HashMap[K, V]()

  def initialize(key: K, value: V): Unit = global.put(key, value)

  def getOrInitialize(key: K, initializer: => V) = {
    get().get(key) match {
      case null =>
        val initialized = global.get(key) match {
          case null =>
            val init = initializer
            global.put(key, init)
            init
          case some => some
        }
        get().put(key, initialized)
        initialized
      case some => some
    }
  }
}
package io.amient.affinity.core.state

import java.util.Observer
import java.util.concurrent.TimeUnit

import akka.actor.{ActorContext, Props}
import akka.serialization.SerializationExtension
import akka.util.Timeout
import io.amient.affinity.core.actor._
import io.amient.affinity.core.serde.primitive.InternalMessage
import io.amient.affinity.core.storage.{LogStorage, Record}
import io.amient.affinity.core.util.{CloseableIterator, Reply, TimeRange}
import io.amient.affinity.core.{Murmur2Partitioner, ack, any2ref}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.postfixOps
import scala.reflect.ClassTag

case class KVGReplace(key: Any, value: Any) extends Routed with Reply[Option[Any]] with InternalMessage

case class KVGDelete(key: Any) extends Routed with Reply[Option[Any]] with InternalMessage

case class KVGInsert(key: Any, value: Any) extends Routed with Reply[Any] with InternalMessage

case class KVGGetAndUpdate(key: Any, f: Option[Any] => Option[Any]) extends Routed with Reply[Option[Any]] with InternalMessage

case class KVGUpdateAndGet(key: Any, f: Option[Any] => Option[Any]) extends Routed with Reply[Option[Any]] with InternalMessage


/**
  * KVStoreGlobal is a state store whose data are replicated locally for reading to every gateway that references it.
  * Global stores can have any number of partitions.
  * Writes will be directed at the elected master while reads will be always local.
  * The futures returned by the write operations will be acked
  *
  * @param identifier name of the global store
  * @param conf       state configuration
  * @param context    parent actor context
  * @tparam K
  * @tparam V
  */
class KVStoreGlobal[K: ClassTag, V: ClassTag](identifier: String, conf: StateConf, context: ActorContext) extends KVStore[K, V] {

  val partitions: Int = if (conf.Partitions.isDefined) conf.Partitions() else if (!conf.External()) {
    throw new IllegalArgumentException(s"Global State Store `$identifier` is not configured either with partitions>=1 or external=true")
  } else if (conf.Storage.Class.isDefined) {
    val storage: LogStorage[_] = LogStorage.newInstance(conf.Storage)
    try {
      storage.getNumPartitions
    } finally {
      storage.close()
    }
  } else {
    throw new IllegalArgumentException(s"External Global State Store `$identifier` doesn't have any storage configured.")
  }

  val partitioner = new Murmur2Partitioner

  val serialization = SerializationExtension(context.system)

  val underlying: List[KVStoreLocal[K, V]] = (0 until partitions)
    .map(partition => KVStoreLocal.create[K, V](identifier, partition, conf, partitions, context.system))
    .toList

  val master = context.actorOf(Props(classOf[Group], identifier, partitions, partitioner))

  val container = context.actorOf(Props(new Container(identifier) {
    underlying.zipWithIndex.foreach { case (store, partition) =>
      context.actorOf(Props(new Partition {
        state[K, V](identifier, store)

        override def handle: Receive = {
          case request@KVGReplace(key, value) => request(sender) ! store.replace(key.asInstanceOf[K], value.asInstanceOf[V])
          case request@KVGDelete(key) => request(sender) ! store.delete(key.asInstanceOf[K])
          case request@KVGInsert(key, value) => request(sender) ! store.insert(key.asInstanceOf[K], value.asInstanceOf[V])
          case request@KVGGetAndUpdate(key, f) => request(sender) ! store.getAndUpdate(key.asInstanceOf[K], (prev: Option[V]) => f(prev).map(_.asInstanceOf[V]))
          case request@KVGUpdateAndGet(key, f) => request(sender) ! store.updateAndGet(key.asInstanceOf[K], (prev: Option[V]) => f(prev).map(_.asInstanceOf[V]))
          case other => throw new IllegalArgumentException("Could not handle: " + other.getClass + " : " + other.toString)
        }
      }), name = partition.toString)
    }
  }), name = identifier)

  implicit val executor = scala.concurrent.ExecutionContext.Implicits.global

  implicit val timeout = Timeout(conf.WriteTimeoutMs(), TimeUnit.MILLISECONDS)

  override def close(): Unit = underlying.foreach(_.close)

  override def iterator: CloseableIterator[Record[K, V]] = {
    new CloseableIterator[Record[K, V]] {
      val iterators: Seq[CloseableIterator[Record[K, V]]] = underlying.map(_.iterator)
      val combined = iterators.map(_.asScala).foldLeft(Iterator[Record[K, V]]())(_ ++ _)

      def close(): Unit = iterators.foreach(_.close)

      override def hasNext: Boolean = combined.hasNext

      override def next(): Record[K, V] = combined.next
    }
  }

  override def iterator(range: TimeRange, prefix: Any*): CloseableIterator[Record[K, V]] = {
    new CloseableIterator[Record[K, V]] {
      val iterators = underlying.map(_.iterator(range, prefix: _*))
      val combined = iterators.map(_.asScala).foldLeft(Iterator[Record[K, V]]())(_ ++ _)

      def close(): Unit = iterators.foreach(_.close)

      override def hasNext: Boolean = combined.hasNext

      override def next(): Record[K, V] = combined.next
    }
  }

  override def apply(key: K): Option[V] = if (partitions == 1) underlying(0).apply(key) else {
    underlying(partitioner.partition(serialization.serialize(any2ref(key)).get, partitions)).apply(key)
  }

  override def range(range: TimeRange, prefix1: Any, prefixN: Any*): Map[K, V] = {
    underlying.map(_.range(range, prefix1, prefixN: _*)).foldLeft(Map[K, V]())(_ ++ _)
  }

  override def numKeys: Long = underlying.map(_.numKeys).sum

  override def replace(key: K, value: V): Future[Option[V]] = master ?? KVGReplace(key, value) map(_.map(_.asInstanceOf[V]))

  override def delete(key: K): Future[Option[V]] = master ?? KVGDelete(key) map(_.map(_.asInstanceOf[V]))

  override def insert(key: K, value: V): Future[V] = master ?? KVGInsert(key, value) map(_.asInstanceOf[V])

  override def getAndUpdate(key: K, f: Option[V] => Option[V]): Future[Option[V]] = {
    master ?? KVGGetAndUpdate(key, (prev: Option[Any]) => f(prev.map(_.asInstanceOf[V]))) map (_.map(_.asInstanceOf[V]))
  }

  override def updateAndGet(key: K, f: Option[V] => Option[V]): Future[Option[V]] = {
    master ?? KVGUpdateAndGet(key, (prev: Option[Any]) => f(prev.map(_.asInstanceOf[V]))) map (_.map(_.asInstanceOf[V]))
  }

  /**
    * @return statistics about the memstore and storage, whatever is available
    */
  override def getStats: String = underlying.map(_.getStats).mkString("\n")

  override def addKeyValueObserver(key: K, observer: Observer): ObservableKeyValue = {
    underlying(partitioner.partition(serialization.serialize(any2ref(key)).get, partitions)).addKeyValueObserver(key, observer)
  }

  override def addKeyValueObserver(key: K, init: Object, observer: Observer): Observer = {
    underlying(partitioner.partition(serialization.serialize(any2ref(key)).get, partitions)).addKeyValueObserver(key, init, observer)
  }

  override def removeKeyValueObserver(key: K, observer: Observer): Unit = {
    underlying(partitioner.partition(serialization.serialize(any2ref(key)).get, partitions)).removeKeyValueObserver(key, observer)
  }

}

package io.amient.affinity.core.state

import java.util.concurrent.TimeUnit

import akka.actor.{ActorContext, Props}
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor._
import io.amient.affinity.core.storage.{State, StateConf}
import io.amient.affinity.core.util.{Reply, TimeRange}

import scala.concurrent.Future
import scala.language.postfixOps
import scala.reflect.ClassTag


/**
  * KVStoreGlobal is a state store whose data are replicated locally for reading to every gateway that references it.
  * Global stores currently must have exactly 1 partition (TODO #201)
  * Writes will be directed at the elected master.
  *
  * @param identifier   name of the global store
  * @param conf         state configuration
  * @param context      parent actor context
  * @tparam K
  * @tparam V
  */
class KVStoreGlobal[K: ClassTag, V: ClassTag](identifier: String, conf: StateConf, context: ActorContext) extends KVStore[K, V] {

  //TODO #122 for the following messages as well
  case class Replace(key: K, value: V) extends Routed with Reply[Option[V]]
  case class Delete(key: K) extends Routed with Reply[Option[V]]
  case class Insert(key: K, value: V) extends Routed with Reply[V]
  case class GetAndUpdate(key: K, f: Option[V] => Option[V]) extends Routed with Reply[Option[V]]
  case class UpdateAndGet(key: K, f: Option[V] => Option[V]) extends Routed with Reply[Option[V]]

  val numPartitions = 1

  val partition = 0

  val underlying: State[K, V] = State.create[K, V](identifier, partition, conf, numPartitions, context.system)

  val group = context.actorOf(Props(classOf[Group], identifier, numPartitions))

  val container = context.actorOf(Props(new Container(identifier) {
    context.actorOf(Props(new Partition {
      state[K,V](identifier, underlying)
      override def handle = {
        case request@Replace(key, value) => request(sender) ! underlying.replace(key, value)
        case request@Delete(key) => request(sender) ! underlying.delete(key)
        case request@Insert(key, value) => request(sender) ! underlying.insert(key, value)
        case request@GetAndUpdate(key, f) => request(sender) ! underlying.getAndUpdate(key, f)
        case request@UpdateAndGet(key, f)=> request(sender) ! underlying.updateAndGet(key, f)
      }
    }), name = partition.toString)
  }), name = identifier)

  implicit val executor = scala.concurrent.ExecutionContext.Implicits.global

  implicit val timeout = Timeout(conf.WriteTimeoutMs(), TimeUnit.MILLISECONDS)

  override def close(): Unit = underlying.close()

  /**
    * get an iterator for all records that are strictly not expired
    *
    * @return a weak iterator that doesn't block read and write operations
    */
  override def iterator = underlying.iterator

  /**
    * get iterator for all records that are within a given time range and an optional prefix sequence
    *
    * @param range  time range to filter the records by
    * @param prefix vararg sequence for the compound key to match; can be empty
    * @return a weak iterator that doesn't block read and write operations
    */
  override def iterator(range: TimeRange, prefix: Any*) = underlying.iterator(range, prefix: _*)

  /**
    * Retrieve a value from the store asynchronously
    *
    * @param key to retrieve value of
    * @return Future.Success(Some(V)) if the key exists and the value could be retrieved and deserialized
    *         Future.Success(None) if the key doesn't exist
    *         Future.Failed(Throwable) if a non-fatal exception occurs
    */
  override def apply(key: K) = underlying.apply(key)

  /**
    * Get all records that match the given time range and optional prefix sequence
    *
    * @param range   time range to filter the records by
    * @param prefix1 mandatory root prefix
    * @param prefixN optional secondary prefix sequence
    * @return Map[K,V] as a transformation of Record.key -> Record.value
    */
  override def range(range: TimeRange, prefix1: Any, prefixN: Any*) = underlying.range(range, prefix1, prefixN: _*)

  /**
    * @return numKeys hint - this may be an approximation, depending on the underlying backend's features
    */
  override def numKeys = underlying.numKeys

  /**
    * replace is a faster operation than update because it doesn't look at the existing value
    * associated with the given key
    *
    * @param key   to update
    * @param value new value to be associated with the key
    * @return Future which if successful holds either:
    *         Success(Some(value)) if the write was persisted
    *         Success(None) if the write was persisted but the operation resulted in the value was expired immediately
    *         Failure(ex) if the operation failed due to exception
    */
  override def replace(key: K, value: V): Future[Option[V]] = group ?? Replace(key, value)

  /**
    * delete the given key
    *
    * @param key to delete
    * @return Future which may be either:
    *         Success(None) if the key was deleted
    *         Failure(ex) if the operation failed due to exception
    */
  override def delete(key: K): Future[Option[V]] = group ?? Delete(key)

  /**
    * insert is a syntactic sugar for update which is only executed if the key doesn't exist yet
    *
    * @param key   to insert
    * @param value new value to be associated with the key
    * @return Future value newly inserted if the key did not exist and operation succeeded, failed future otherwise
    */
  override def insert(key: K, value: V): Future[V] = group ?? Insert(key, value)

  /**
    * atomic get-and-update if the current and updated value are the same the no modifications are made to
    * the underlying stores and the returned future is completed immediately.
    *
    * @param key to updateImpl
    * @param f   function which given a current value returns an updated value or empty if the key is to be removed
    *            as a result of the update
    * @return Future Optional of the value previously held at the key position
    */
  override def getAndUpdate(key: K, f: Option[V] => Option[V]): Future[Option[V]] = group ?? GetAndUpdate(key, f)

  /**
    * atomic update-and-get - if the current and updated value are the same the no modifications are made to
    * the underlying stores and the returned future is completed immediately.
    *
    * @param key key which is going to be updated
    * @param f   function which given a current value returns an updated value or empty if the key is to be removed
    *            as a result of the update
    * @return Future optional value which will be successful if the put operation succeeded and will hold the updated value
    */
  override def updateAndGet(key: K, f: Option[V] => Option[V]): Future[Option[V]] = group ?? UpdateAndGet(key, f)

}

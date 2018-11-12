package io.amient.affinity.core.state

import java.io.Closeable

import io.amient.affinity.core.storage.Record
import io.amient.affinity.core.util.{CloseableIterator, TimeRange}

import scala.concurrent.Future

trait KVStore[K, V] extends Closeable with ObservableKVStore[K] {

  /**
    * get an iterator for all records that are strictly not expired
    *
    * @return a weak iterator that doesn't block read and write operations
    */
  def iterator: CloseableIterator[Record[K, V]]

  /**
    * get iterator for all records that are within a given time range and an optional prefix sequence
    *
    * @param range  time range to filter the records by
    * @param prefix vararg sequence for the compound key to match; can be empty
    * @return a weak iterator that doesn't block read and write operations
    */
  def iterator(range: TimeRange, prefix: Any*): CloseableIterator[Record[K, V]]

  /**
    * Retrieve a value from the store asynchronously
    *
    * @param key to retrieve value of
    * @return Future.Success(Some(V)) if the key exists and the value could be retrieved and deserialized
    *         Future.Success(None) if the key doesn't exist
    *         Future.Failed(Throwable) if a non-fatal exception occurs
    */
  def apply(key: K): Option[V]

  /**
    * Get all records that match the given time range and optional prefix sequence
    *
    * @param range   time range to filter the records by
    * @param prefix1 mandatory root prefix
    * @param prefixN optional secondary prefix sequence
    * @return Map[K,V] as a transformation of Record.key -> Record.value
    */
  def range(range: TimeRange, prefix1: Any, prefixN: Any*): Map[K, V]

  /**
    * @return numKeys hint - this may be an approximation, depending on the underlying backend's features
    */
  def numKeys: Long

  /**
    * @return statistics about the memstore and storage, whatever is available
    */
  def getStats: String

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
  def replace(key: K, value: V): Future[Option[V]]

  /**
    * update is a syntactic sugar for update where the value is always overriden unless the current value is the same
    *
    * @param key   to updateImpl
    * @param value new value to be associated with the key
    * @return Future Optional of the value previously held at the key position
    */
  final def update(key: K, value: V): Future[Option[V]] = getAndUpdate(key, _ => Some(value))

  /**
    * delete the given key
    *
    * @param key to delete
    * @return Future which may be either:
    *         Success(None) if the key was deleted
    *         Failure(ex) if the operation failed due to exception
    */
  def delete(key: K): Future[Option[V]]

  /**
    * remove is a is a syntactic sugar for update where None is used as Value
    * it is different from delete in that it returns the removed value
    * which is more costly.
    *
    * @param key to remove
    * @return Future Optional of the value previously held at the key position
    */
  final def remove(key: K): Future[Option[V]] = getAndUpdate(key, _ => None)

  /**
    * insert is a syntactic sugar for update which is only executed if the key doesn't exist yet
    *
    * @param key   to insert
    * @param value new value to be associated with the key
    * @return Future value newly inserted if the key did not exist and operation succeeded, failed future otherwise
    */
  def insert(key: K, value: V): Future[V]

  /**
    * atomic get-and-update if the current and updated value are the same the no modifications are made to
    * the underlying stores and the returned future is completed immediately.
    *
    * @param key to updateImpl
    * @param f   function which given a current value returns an updated value or empty if the key is to be removed
    *            as a result of the update
    * @return Future Optional of the value previously held at the key position
    */
  def getAndUpdate(key: K, f: Option[V] => Option[V]): Future[Option[V]]

  /**
    * atomic update-and-get - if the current and updated value are the same the no modifications are made to
    * the underlying stores and the returned future is completed immediately.
    *
    * @param key key which is going to be updated
    * @param f   function which given a current value returns an updated value or empty if the key is to be removed
    *            as a result of the update
    * @return Future optional value which will be successful if the put operation succeeded and will hold the updated value
    */
  def updateAndGet(key: K, f: Option[V] => Option[V]): Future[Option[V]]

}

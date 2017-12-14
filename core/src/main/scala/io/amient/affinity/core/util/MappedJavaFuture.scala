package io.amient.affinity.core.util

import java.util.concurrent.{Future, TimeUnit}

abstract class MappedJavaFuture[A, T](f: Future[A]) extends Future[T] {

  override def isCancelled: Boolean = f.isCancelled

  def map(result: A): T

  override def get(): T = map(f.get)

  override def get(timeout: Long, unit: TimeUnit): T = map(f.get(timeout, unit))

  override def cancel(mayInterruptIfRunning: Boolean): Boolean = f.cancel(mayInterruptIfRunning)

  override def isDone: Boolean = f.isDone
}

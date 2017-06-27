/*
 * Copyright 2016 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.affinity.core.util

import java.util.concurrent.TimeoutException

import akka.AkkaException
import akka.actor.{ActorRef, Scheduler, Status}
import akka.pattern.{after, ask}
import akka.util.Timeout

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.{ClassTag, classTag}
import scala.runtime.BoxedUnit
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * These are utilities for stateless Akka Ack pattern.
  * They are used where a chain of events has to be guaranteed to have completed.
  * For example, Coordinator identifies a new master and sends AddMaster to the respective Gateway.
  * Gateway in turn sends an ack message to the given Partition to BecomeMaster and
  * the partition must in turn ack that it has completed the transition succesfully.
  * The response ack returns back up the chain until Coordinator is sure that the
  * partition has transitioned its state and knows it is now a Master.
  *
  * Supported delivery semantics: At-Least-Once
  *
  * Because this ack implementation is stateless, any logic relying on its functionality
  * must take care of deduplication that can result from the retries in case the ack response
  * wasn't delivered.
  *
  * Likewise in-order processing must be taken care of by the code relying on the ack.
  */
trait AckSupport {

  implicit def ack(actorRef: ActorRef): AckableActorRef = new AckableActorRef(actorRef, maxRetries = 3)

}

trait Reply[+T]

final class AckableActorRef(val target: ActorRef, val maxRetries: Int = 3) extends AnyRef {

  /**
    * initiator ack() which is used where the guaranteed processin of the message is required
    * from the target actor.
    *
    * @param message
    * @param timeout
    * @param context
    * @return
    */
  def ack[T](message: Reply[T])(implicit timeout: Timeout, scheduler: Scheduler, context: ExecutionContext, tag: ClassTag[T]): Future[T] = {
    val promise = Promise[T]()
    def attempt(retry: Int, delay: Duration = 0 seconds): Unit = {
      val f = if (delay.toMillis == 0) target ? message else after(timeout.duration, scheduler)(target ? message)
      f map {
        case result: T => promise.success(result)
        case result: BoxedUnit if (tag == classTag[Unit]) => promise.success(().asInstanceOf[T])
        case i => promise.failure(new RuntimeException(s"expecting $tag, got: ${i.getClass} for $message sent to $target"))
      } recover {
        case cause: AkkaException => promise.failure(cause)
        case cause: IllegalArgumentException => promise.failure(cause)
        case cause: NoSuchElementException => promise.failure(cause)
        case cause if (retry == 0) => promise.failure(cause)
        case _: TimeoutException => attempt(retry - 1)
        case _ => attempt(retry - 1, timeout.duration)
      }
    }
    attempt(maxRetries)
    promise.future
  }

  /**
    * Intermediate reply with future. An ack is sent to the `target` actor when the future completes.
    * @param request message which is being replied to
    * @param closure which must return future on which the acknowledgement depends
    * @tparam T
    */

  def replyWith[T](request: Reply[T])(closure: => Future[T])(implicit context: ExecutionContext): Unit = {
    try {
      val f: Future[T] = closure
      f onComplete {
        case Success(result) => target ! result
        case Failure(e) => target ! Status.Failure(e)
      }
    } catch {
      case NonFatal(e) => target ! Status.Failure(e)
    }
  }

  /**
    * end of chain reply which runs the given closure and reports either a success or failure
    * back to the target requester.
    *
    * @param request message which is being replied to
    * @param closure of which result will be send as the acknowledgement value
    */
  def reply[T](request: Reply[T])(closure: => T): Unit = {
    try {
      val result: T = closure
      target ! result
    } catch {
      case NonFatal(e) => target ! Status.Failure(e)
    }
  }

}
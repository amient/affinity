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

package io.amient.affinity.core

import java.util.concurrent.TimeoutException

import akka.actor.{ActorRef, Scheduler, Status}
import akka.pattern.ask
import akka.pattern.after
import akka.util.Timeout

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.reflect.classTag
import scala.runtime.BoxedUnit
import scala.util.control.NonFatal
import scala.util.{Failure, Success}
import scala.concurrent.duration._

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
package object ack {

  trait Reply[+T]

  /**
    * initiator ack() which is used where the guaranteed processin of the message is required
    * from the target actor.
    *
    * @param target
    * @param message
    * @param timeout
    * @param context
    * @return
    */
  def ack[T](target: ActorRef, message: Reply[T])(implicit timeout: Timeout, scheduler: Scheduler, context: ExecutionContext, tag: ClassTag[T]): Future[T] = {
    val maxRetries = 3
    val promise = Promise[T]()
    def attempt(retry: Int, delay: Duration = 0 seconds): Unit = {
      val f = if (delay.toMillis == 0) target ? message else after(timeout.duration, scheduler)(target ? message)
      f map {
        case result: T => promise.success(result)
        case result: BoxedUnit if (tag == classTag[Unit]) => promise.success(().asInstanceOf[T])
        case i =>
          promise.failure(new RuntimeException(s"expecting $tag, got: ${i.getClass} for $message sent to $target"))
      } recover {
        case cause if (retry == 0) => promise.failure(cause)
        case cause: TimeoutException => attempt(retry - 1)
        case x => attempt(retry - 1, timeout.duration)
      }
    }
    attempt(maxRetries)
    promise.future
  }

  /**
    * Intermediate ack with future. An ack is sent to the `replyTo` actor when the future completes.
    * @param request which is being replied to
    * @param replyTo sender who sent the request and expects the reply
    * @param closure which must return future on which the acknowledgement depends
    * @tparam T
    */

  def replyWith[T](request: Reply[T], replyTo: ActorRef)(closure: => Future[T])(implicit context: ExecutionContext): Unit = {
    val f: Future[T] = closure
    f onComplete {
      case Success(result) => replyTo ! result
      case Failure(e) => replyTo ! Status.Failure(e)
    }
  }

  /**
    * end of chain ack() which runs the given closure and reports either a success or failure
    * back.
    *
    * @param replyTo an actor which required the ack. this actor will receive the result of the closure
    * @param closure of which result will be send as acknowledgement
    */
  def reply[T](replyTo: ActorRef)(closure: => T): Unit = {
    try {
      val result: T = closure
      replyTo ! result
    } catch {
      case NonFatal(e) => replyTo ! Status.Failure(e)
    }
  }


}

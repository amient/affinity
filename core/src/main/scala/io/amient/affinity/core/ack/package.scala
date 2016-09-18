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

import akka.actor.{ActorRef, Status}
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import io.amient.affinity.core.ack.AckDuplicate

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
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
  * must take care of deduplication by throwing AckDuplicate form the closure code
  * that needs to be guaranteed executed exactly once.
  *
  * Likewise in-order processing must be taken care of by the code relying on the ack.
  */
package object ack {

  final case class AckDuplicate(e: Throwable) extends RuntimeException(e)

  /**
    * end of chain ack() which runs the given closure and reports either a success or failure
    * back.
    *
    * @param replyTo
    * @param closure
    */
  def ack[T](replyTo: ActorRef)(closure: => T): Unit = {
    try {
      val result: T = closure
      replyTo ! Status.Success(result)
    } catch {
      case AckDuplicate(e) => replyTo ! Status.Success(true)
      case NonFatal(e) => replyTo ! Status.Failure(e)
    }
  }

  /**
    * intermediate ack() which requests ack from the target for given message and reports it
    * back to the replyTo up the chain.
    *
    * @param target
    * @param message
    * @param replyTo
    * @param context
    */
  def ack(target: ActorRef, message: Any, replyTo: ActorRef)(implicit context: ExecutionContext): Unit = {
    ack(target, message) onComplete {
      case Failure(e) => replyTo ! Status.Failure(e)
      case Success(result) => replyTo ! result
    }
  }

  /**
    * initiator ack() which is used where the guaranteed processin of the message is required
    * from the target actor.
    * @param target
    * @param message
    * @param context
    * @return
    */
  def ack(target: ActorRef, message: Any)(implicit context: ExecutionContext): Future[Any] = {
    //TODO ACK - configurable ack retries and timeout
    implicit val numRetries: Int = 3
    implicit val timetout = Timeout(6 second)
    val promise = Promise[Any]()

    def attempt(retry: Int): Unit = {
      target ? message onComplete {
        case Success(result) => promise.success(result)
        case Failure(cause) => {
          cause match {
            case to: AskTimeoutException if (to.getCause() == null) => promise.failure(cause)
            case _ => if (retry == 0) promise.failure(cause) else attempt(retry - 1)
          }
        }
      }
    }
    attempt(numRetries)
    promise.future
  }
}

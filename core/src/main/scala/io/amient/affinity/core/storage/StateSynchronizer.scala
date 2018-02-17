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

package io.amient.affinity.core.storage

import io.amient.affinity.core.util.EventTime
import io.amient.affinity.core.util.TimeRange
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.Closeable
import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.util.Iterator
import java.util.Optional
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicReference

final class StateSynchronizer(storage: LogStorage[_], memstore: MemStore) extends Thread with Closeable {

  private var state: ObservableState[_] = null

  def start(state: ObservableState[_]): Unit = {
    this.state = state
    start()
  }


  final val log = LoggerFactory.getLogger(classOf[StateSynchronizer])

  //    final public String id;
  //    final public int partition;
  //    final Thread consumer;
  //
  //    private AtomicReference<Throwable> consumerError = new AtomicReference<>(null);
  //    volatile private ObservableState<?> state = null;
  //    volatile private boolean consuming = false;
  //    volatile private boolean tailing = true;
  //

  override def run() {
    try {

      //      while (true) {
      //
      //        if (isInterrupted()) throw new InterruptedException();
      //
      //        consuming = true;
      //
      //        while (consuming) {
      //
      //          if (isInterrupted()) throw new InterruptedException();
      //
      //          try {
      //            Iterator < BinaryRecordAndOffset > records = stream.fetch();
      //            if (records == null) {
      //              consuming = false;
      //            } else while (records.hasNext()) {
      //              BinaryRecordAndOffset r = records.next();
      //              //for tailing state it means either it is a) replica b) external
      //              if (r.value == null) {
      //                memstore.unload(r.key, r.offset);
      //                if (tailing) state.internalPush(r.key, Optional.empty());
      //              } else {
      //                memstore.load(r.key, r.value, r.offset, r.timestamp);
      //                if (tailing) state.internalPush(r.key, Optional.of(r.value));
      //              }
      //            }
      //            //                                if (!tailing && lastProcessedOffset >= endOffset) {
      //            //                                    consuming = false
      //            //                                }
      //          } catch (Throwable e) {
      //            synchronized(Storage.this) {
      //              consumerError.set(e);
      //              Storage.this.notify(); //boot failure
      //            }
      //          }
      //        }
      //
      //        synchronized(Storage.this) {
      //          Storage.this.notify(); //boot complete
      //          Storage.this.wait(); //wait for tail instruction
      //        }
      //      }
      //
      //    } catch (InterruptedException e) {
      //      return;
      //    }
      //    catch (Throwable e) {
      //      consumerError.set(e);
    } finally {
      //          try {
      //            stream.close();
      //          } catch (IOException e) {
      //            e.printStackTrace();
      //          }
    }
    //
  }

  //  /**
  //    * The implementation should stop listening for updates on the underlying topic after it has
  //    * fully caught up with the state updates. This method must block until completed and can
  //    * be interrupted by a consequent tail() call.
  //    * <p>
  //    * Once this method returns, the partition or service which owns it will become available
  //    * for serving requests and so the state must be in a fully consistent state.
  //    */
  //  public void boot() {
  //    //        consumer.synchronized {
  //    //            if (tailing) {
  //    //                tailing = false
  //    //                while (true) {
  //    //                    consumer.wait(1000)
  //    //                    if (consumerError.get != null) {
  //    //                        consumer.kafkaConsumer.wakeup()
  //    //                        throw consumerError.get
  //    //                    } else if (!consuming) {
  //    //                        return
  //    //                    }
  //    //                }
  //    //            }
  //    //        }
  //  }

  //
  //  /**
  //    * the implementation should start listening for updates and keep the memstore up to date.
  //    * The tailing must be done asychrnously - this method must return immediately and be idempotent.
  //    */
  //  public void tail() {
  //    //        consumer.synchronized {
  //    //            if (!tailing) {
  //    //                tailing = true
  //    //                consumer.notify
  //    //            }
  //    //        }
  //  }
  //
  /**
    * close all resource and background processes used by the memstore
    * implementations of Storage should override this method and first close their specific resources
    * and then call super.close()
    */
  def close() {
    //            consumer.interrupt()
    //            consumer.kafkaConsumer.wakeup()
    //            if (!readonly) kafkaProducer.close
  }

}

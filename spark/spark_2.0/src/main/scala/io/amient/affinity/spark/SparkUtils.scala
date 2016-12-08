///*
// * Copyright 2016 Michal Harish, michal.harish@gmail.com
// *
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.amient.util.spark
//
//import java.text.SimpleDateFormat
//
//import org.apache.spark.HashPartitioner
//import org.apache.spark.rdd.RDD
//
//import scala.reflect.ClassTag
//
//trait SparkUtils {
//
//  val df = new SimpleDateFormat("yyyy-MM-dd HH:mm")
//
//  def t(): Long = System.currentTimeMillis()
//
//  def time[A](a: => A): A = {
//    val l = t()
//    val r = a
//    println((t() - l).toDouble / 1000)
//    r
//  }
//
//  def t(formattedDate: String): Long = df.parse(formattedDate).getTime
//
//  def d(numDaysAgo: Int) = df.format(System.currentTimeMillis() - 86400000L * numDaysAgo)
//
//  def dist(rdd: RDD[_]) = {
//    rdd.mapPartitionsWithIndex((part, iter)=> Array((part, iter.size)).iterator, true).collect
//  }
//
//  def hist[X](rdd: RDD[(X, Long)]): Array[(Long, Long)] = {
//    rdd.map(v => (v._2, 1L)).aggregateByKey(0L)(_ + _, _ + _).sortByKey().collect
//  }
//
//  def compact[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)])(implicit ordering: Ordering[K]): RDD[(K, V)] = {
//    reduceToLatest(rdd.repartitionAndSortWithinPartitions(new HashPartitioner(rdd.partitions.size)))
//  }
//
//  def reduceToLatest[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): RDD[(K, V)] = {
//    rdd.mapPartitions(
//      part => {
//        new Iterator[(K, V)] {
//          var current: (K, V) = null
//
//          seek
//
//          override def hasNext: Boolean = current != null
//
//          override def next(): (K, V) = {
//            current match {
//              case null => throw new NoSuchElementException
//              case ret => {
//                seek
//                ret
//              }
//            }
//          }
//
//          private def seek() {
//            val prev = current
//            while (part.hasNext) {
//              current = part.next
//              if (prev == null || prev._1 != current._1) {
//                return
//              }
//            }
//            current = null
//          }
//        }
//      }
//    )
//  }
//}

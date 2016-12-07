/**
  * SparkUtils
  * Copyright (C) 2015 Michal Harish
  * <p/>
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  * <p/>
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  * <p/>
  * You should have received a copy of the GNU General Public License
  * along with this program.  If not, see <http://www.gnu.org/licenses/>.
  */

package io.amient.util.spark

import java.text.SimpleDateFormat
import java.util.UUID

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait SparkUtils {

  val df = new SimpleDateFormat("yyyy-MM-dd HH:mm")

  def t(): Long = System.currentTimeMillis()

  def time[A](a: => A): A = {
    val l = t()
    val r = a
    println((t() - l).toDouble / 1000)
    r
  }

  def t(formattedDate: String): Long = df.parse(formattedDate).getTime

  def d(numDaysAgo: Int) = df.format(System.currentTimeMillis() - 86400000L * numDaysAgo)

  def dist(rdd: RDD[_]) = {
    rdd.mapPartitionsWithIndex((part, iter)=> Array((part, iter.size)).iterator, true).collect
  }

  def hist[X](rdd: RDD[(X, Long)]): Array[(Long, Long)] = {
    rdd.map(v => (v._2, 1L)).aggregateByKey(0L)(_ + _, _ + _).sortByKey().collect
  }

  def compact[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)])(implicit ordering: Ordering[K]): RDD[(K, V)] = {
    reduceToLatest(rdd.repartitionAndSortWithinPartitions(new HashPartitioner(rdd.partitions.size)))
  }

  def reduceToLatest[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): RDD[(K, V)] = {
    rdd.mapPartitions(
      part => {
        new Iterator[(K, V)] {
          var current: (K, V) = null

          seek

          override def hasNext: Boolean = current != null

          override def next(): (K, V) = {
            current match {
              case null => throw new NoSuchElementException
              case ret => {
                seek
                ret
              }
            }
          }

          private def seek() {
            val prev = current
            while (part.hasNext) {
              current = part.next
              if (prev == null || prev._1 != current._1) {
                return
              }
            }
            current = null
          }
        }
      }
    )
  }
}

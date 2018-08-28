/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
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

package affinity

import akka.http.scaladsl.model.HttpMethods
import akka.util.Timeout
import io.amient.affinity.avro.record.{AvroRecord, Fixed}
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.{GatewayHttp, GatewayStream, Partition, Routed}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.storage.Record
import io.amient.affinity.core.util._

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps



object ESecondaryIndexMain extends App {
  new Node("example.conf").start()
}



case class Author(username: String) extends AvroRecord {
  val id = username.hashCode
}

case class Article(title: String, timestamp: Long) extends AvroRecord with EventTime {
  override def eventTimeUnix() = timestamp
}



class ESecondaryIndex extends GatewayStream with GatewayHttp {

  implicit val executor = context.dispatcher
  implicit val scheduler = context.system.scheduler
  implicit val timeout = Timeout(5 seconds)

  val ks = keyspace("articles")

  input[Author, Article]("input-stream") { record =>
    ks ?! StoreArticle(record.key, record.value)
  }

  override def handle: Receive = {
    case HTTP(HttpMethods.GET, PATH("articles", username), _, response) =>
      ks ?! GetAuthorArticles(Author(username), 0L) map (handleAsJson(response, _))

    case HTTP(HttpMethods.GET, PATH("words", word), _, response) =>
      ks ?? GetWordIndex(word, 0L) map (handleAsJson(response, _))

    case HTTP(HttpMethods.GET, PATH("words-since", word), _, response) =>
      ks ?? GetWordIndex(word, 1530086400000L) map (handleAsJson(response, _))

    case HTTP(HttpMethods.GET, PATH("delete-articles-containing", word), _, response) =>
      ks ?? DeleteArticles(word) map (accept(response, _))

  }

}



case class GetWordIndex(word: String, since: Long) extends ScatterIterable[Article]

case class DeleteArticles(word: String) extends ScatterUnit

sealed trait Authored extends Routed {
  val author: Author

  override def key: Any = author.id
}

case class StoreArticle(author: Author, article: Article) extends AvroRecord with Authored with Reply[Unit]

case class StorageKey(@Fixed authorId: Int, auto: Int) extends AvroRecord

case class GetAuthorArticles(author: Author, since: Long) extends AvroRecord with Authored with Reply[Seq[Article]]




class ArticlesPartition extends Partition {

  val articles = state[StorageKey, Article]("articles")

  val wordindex = articles.index("words") { record: Record[_, Article] =>
    record.value.title.split("\\s").toList.map(_.trim.toLowerCase)
  }

  implicit val executor = context.dispatcher

  override def handle: Receive = {

    case request@GetAuthorArticles(author, since) =>
      request(sender) ! articles.range(TimeRange.since(since), author.id).values.toList

    case request@GetWordIndex(word, since) =>
      request(sender) ! wordindex(word.trim.toLowerCase, TimeRange.since(since))(_.toList).map(articles.apply).flatten

    case request@DeleteArticles(word) =>
      val deleted = Future.sequence(wordindex(word.trim.toLowerCase, TimeRange.UNBOUNDED)(_.map(articles.delete)))
      request(sender) ! deleted.map(_ => ())

    case request@StoreArticle(author, article) => request(sender) ! {
      articles.lockAsync(author) {
        //using an async lock because .replace is asynchronous so another request may get processed and miscount
        val articlesSoFar = articles.iterator(TimeRange.UNBOUNDED, author.id)
        try {
          val nextAuto = if (!articlesSoFar.hasNext) 1 else articlesSoFar.asScala.map(_.key.auto).max + 1
          val key = StorageKey(author.id, nextAuto)
          articles.replace(key, article).map { _ =>
            //this is only here to tell the test the fixtures were all processed
            context.system.eventStream.publish(request)
          }
        } finally {
          //raw memstore iterators need closing to free resources
          articlesSoFar.close
        }
      }
    }

  }
}

package com.rallyhealth.driver

import com.mongodb.client.result.DeleteResult
import com.rallyhealth.driver.bench.BoxPersistence
import org.bson.codecs.configuration.CodecRegistries._
import org.bson.codecs.configuration.{CodecRegistries, CodecRegistry}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}
import org.joda.time.DateTime
import org.mongodb.scala._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.model.Accumulators._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.Try


class JodaCodec extends Codec[DateTime] {

  override def decode(bsonReader: BsonReader, decoderContext: DecoderContext): DateTime = new DateTime(bsonReader.readDateTime())

  override def encode(bsonWriter: BsonWriter, t: DateTime, encoderContext: EncoderContext): Unit = bsonWriter.writeDateTime(t.getMillis)

  override def getEncoderClass: Class[DateTime] = classOf[DateTime]
}

class ScalaDriverBoxConnectionManager {

  val mongoClient: MongoClient = MongoClient()
  val database: MongoDatabase = mongoClient.getDatabase("scalaDriverDb")
  val codecRegistry: CodecRegistry = fromRegistries(fromProviders(createCodecProvider[CorrugatedBox]), CodecRegistries.fromCodecs(new JodaCodec), DEFAULT_CODEC_REGISTRY)
  val collection: MongoCollection[CorrugatedBox] = database.getCollection[CorrugatedBox]("box").withCodecRegistry(codecRegistry)
}

class ScalaDriverBoxPersistence(collection: MongoCollection[CorrugatedBox]) extends BoxPersistence[Completed, DeleteResult] {

  def save(box: CorrugatedBox): Future[Completed] = {
    collection.insertOne(box).toFuture
  }

  def findOneCorrugatedBox(): Future[Option[CorrugatedBox]] = {

    import org.mongodb.scala.model.Filters.{eq => eqTo}
    collection
      .find(eqTo("length", 1))
      .toFuture()
      .map(_.headOption)
  }

  def findCorrugatedBoxById(id: String): Future[Option[CorrugatedBox]] = {

    import org.mongodb.scala.model.Filters.{eq => eqTo}
    collection
      .find(eqTo("_id", id))
      .toFuture()
      .map(_.headOption)
  }

  def deleteAll(): Future[DeleteResult] = {
    collection.deleteMany(org.mongodb.scala.model.Filters.exists("length")).toFuture
  }

  def findAllBoxesSortedByLength(): Future[Seq[Box]] = {
    collection
      .find().sort(ascending("length"))
      .toFuture()
  }

  def findAggregateLength(): Future[Int] = {
    val promise = Promise[Int]()
    collection.aggregate[Document](List(project(and(excludeId(), include("length"))), group(null, sum("total", "$length")))).subscribe(
      new Observer[Document] {
        override def onError(e: Throwable): Unit = promise.failure(new Exception("Failed to aggregate :" + e.getMessage))

        override def onComplete(): Unit = ()

        override def onNext(result: Document): Unit = promise.complete(Try(result.getInteger("total")))
      }
    )
    promise.future
  }
}

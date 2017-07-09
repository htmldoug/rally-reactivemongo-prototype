package com.rallyhealth.driver


import com.rallyhealth.driver.bench.BoxPersistence
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.WriteResult
import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}
import reactivemongo.bson.{BSONDocument, BSONString}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

class ReactiveBoxConnectionManager() {

  val mongoUri = "mongodb://localhost:27017/reactiveProto"

  val driver = MongoDriver()

  val parsedUri: Try[MongoConnection.ParsedURI] = MongoConnection.parseURI(mongoUri)

  val connection: Try[MongoConnection] = parsedUri.map(driver.connection)

  val futureConnection: Future[MongoConnection] = Future.fromTry(connection)

  def db1: Future[DefaultDB] = futureConnection.flatMap(_.database("reactiveproto"))

  def boxCollection: Future[BSONCollection] = db1.map[BSONCollection](database => database.collection("box"))
}

class ReactiveMongoBoxPersistence(collection: BSONCollection) extends BoxPersistence[WriteResult, WriteResult] {

  def save(box: CorrugatedBox): Future[WriteResult] = {
    collection.insert(box)
  }

  def findOneCorrugatedBox(): Future[Option[CorrugatedBox]] = {
    val query = BSONDocument("length" -> 1)
    collection.find(query).one[CorrugatedBox]
  }

  def findCorrugatedBoxById(id: String): Future[Option[CorrugatedBox]] = {
    val query = BSONDocument("_id" -> id)
    collection.find(query).one[CorrugatedBox]
  }

  def findAllBoxesSortedByLength(): Future[List[CorrugatedBox]] = {
    collection.find(BSONDocument()).sort(BSONDocument("length" -> 1)).cursor[CorrugatedBox]().collect[List](25)
  }

  def findAggregateLength(): Future[Int] = {

    import collection.BatchCommands.AggregationFramework.{Group, SumField, Project}

    val res = collection.aggregate(
      Project(BSONDocument("_id" -> 0, "length" -> 1)),
      List(Group(BSONString("null"))("TotalLength" -> SumField("length")))
    )

    res.map {
      results =>
        results.head.head.getAs[Int]("TotalLength").getOrElse(0)
    }
  }

  def deleteAll(): Future[WriteResult] = {
    collection.remove(BSONDocument())
  }
}

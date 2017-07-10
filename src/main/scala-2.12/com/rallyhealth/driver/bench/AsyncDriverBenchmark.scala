package com.rallyhealth.driver.bench

import com.rallyhealth.driver._
import org.joda.time.DateTime
import org.openjdk.jmh.annotations._
import reactivemongo.bson.BSONObjectID

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * ==Quick Run from sbt==
  *
  * > jmh:run -i 10 -wi 5 -f1 -t1 .*
  *
  * Which means "10 iterations" "5 warmup iterations" "1 fork" "1 thread".
  * Benchmarks should be usually executed at least in 10 iterations (as a rule of thumb), but more is better.
  *
  *
  * ==Using Oracle Flight Recorder==
  *
  * Flight Recorder / Java Mission Control is an excellent tool shipped by default in the Oracle JDK distribution.
  * It is a profiler that uses internal APIs (commercial) and thus is way more precise and detailed than your every-day profiler.
  *
  * To record a Flight Recorder file from a JMH run, run it using the jmh.extras.JFR profiler:
  * > jmh:run -prof jmh.extras.JFR -t1 -f 1 -wi 10 -i 20 .*
  *
  * This will result in flight recording file which you can open and analyze offline using JMC.
  * Start with "jmc" from a terminal.
  *
  * @see https://github.com/ktoso/sbt-jmh
  */
class AsyncDriverBenchmark {

  import AsyncDriverBenchmark._

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  def saveThenRead(state: BenchmarkState): immutable.IndexedSeq[Option[CorrugatedBox]] = {
    import state._

    val futures = for (i <- 1 to 1000) yield {
      val id = BSONObjectID.generate().stringify

      val inputBox = CorrugatedBox(id, length = 1, width = 1, height = 1, manufactureDate = DateTime.now, lastShipped = None, layers = 4)

      for {
        saved <- persister.save(inputBox)
        foundBox <- persister.findCorrugatedBoxById(id)
      } yield {
        assert(foundBox.get == inputBox)
        foundBox
      }
    }

    val combinedFuture = Future.sequence(futures)
    Await.result(combinedFuture, patience)
  }
}

object AsyncDriverBenchmark {

  /**
    * Sometimes you way want to initialize some variables that your benchmark code needs,
    * but which you do not want to be part of the code your benchmark measures.
    *
    * Such variables are called "state" variables.
    * State variables are declared in special state classes, and an instance of that
    * state class can then be provided as parameter to the benchmark method.
    *
    * @see http://java-performance.info/jmh/
    */
  @State(Scope.Benchmark)
  class BenchmarkState {

    val patience: FiniteDuration = 60.seconds

    @Param(Array("rm", "smd"))
    var impl: String = _

    var persister: BoxPersistence[_, _] = _
    var onTeardown: () => Unit = _

    @Setup
    def setup(): Unit = {
      impl match {
        case "rm" =>
          val cm = new ReactiveBoxConnectionManager()
          val collection = Await.result(cm.boxCollection, patience)
          persister = new ReactiveMongoBoxPersistence(collection)
          onTeardown = () => {
            cm.driver.close()
          }
        case "smd" =>
          val cm = new ScalaDriverBoxConnectionManager()
          persister = new ScalaDriverBoxPersistence(cm.collection)
          onTeardown = () => {
            cm.mongoClient.close()
          }
      }
      Await.result(persister.deleteAll(), patience)
    }

    @TearDown
    def teardown(): Unit = onTeardown()
  }

}

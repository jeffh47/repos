package repos

import java.util.UUID

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.stream.ActorMaterializer
import akka.testkit.TestProbe
import org.scalatest.{Inside, LoneElement, MustMatchers}
import repos.jdbc.{JdbcDb, TableJanitor}
import repos.{Database, EntryTableRecord}
import repos.jdbc.TableJanitor.{Gap, State}
import repos.testutils.TestUtils._
import repos.testutils.{FooId, FooRepo, TestUtils}

import scala.concurrent.ExecutionContext.Implicits.global

class IndexCleanUpSpec extends org.scalatest.fixture.WordSpec with MustMatchers with LoneElement with Inside {
  spec =>

  type FixtureParam = CatchUpTest

  class CatchUpTest {
    val id1 = FooId(UUID.randomUUID())
    val id2 = FooId(UUID.randomUUID())
    val id3 = FooId(UUID.randomUUID())
    val id4 = FooId(UUID.randomUUID())
    val id5 = FooId(UUID.randomUUID())
    val id6 = FooId(UUID.randomUUID())
    val id7 = FooId(UUID.randomUUID())
    val id8 = FooId(UUID.randomUUID())
    val d1 = "123"
    val d2 = "12345"
    val d3 = "abcde"
    val d4 = "12345678"
    val d5 = "!@#$%^&*"
    val d6 = "ABCDEFGH"
    val d7 = "Foo"
    val h2 = TestUtils.makeH2DB()
    val db = TestUtils.makeH2JdbcDb(h2)

    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    def withTestProbe[T](code: TestProbe => T) = {
      val testProbe = TestProbe()(system)
      try {
        code(testProbe)
      } finally {
        testProbe.ref ! PoisonPill
      }
    }

    def insertNoIndex(on: JdbcDb)(entries: (FooId, String)*) = {
      await(
        on.innerRepo(FooRepo).insert(entries, skipIndexesForTesting = true))
    }

    def createTable(on: JdbcDb = db) = {
      import on.profile.api._
      on.jc.blockingWrapper(on.jc.JanitorIndexStatus.schema.create)
      await(on.run(FooRepo.create()))
      IndexUtil.populateData1(db)
    }
  }

  override def withFixture(test: OneArgTest) = {
    val t = new CatchUpTest
    try {
      test(t)
    } finally {
      t.materializer.shutdown()
      TestUtils.await(t.system.terminate())
      TestUtils.await(t.h2.shutdown)
    }
  }

  "indexCleanUpSpec" must {
    "be able to catch all indexes from scratch" in {
      t =>
        import t._
        t.createTable()
        val statusTable = Map.empty[String, Long].withDefaultValue(-1L)

        val r = TableJanitor.catchUpForRepo(db, db, System.currentTimeMillis(), statusTable, FooRepo,
          State(0, 0, Vector.empty))
        //r must be(State(7, 7, Vector.empty))
        val newStatus = TableJanitor.loadJanitorIndexStatus(db)
        /*newStatus must be(Map(
          "ix3_foo__text_text" -> 7,
          "ix3_foo__len_index" -> 7,
          "ix3_foo__first_ch" -> 7,
          "ix3_foo__first_two_ch" -> 7,
          "ix3_foo__seq" -> 7
        ))*/
    }
  }
}

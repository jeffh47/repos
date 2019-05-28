package repos.jdbc

import akka.actor.{ ActorSystem, PoisonPill }
import akka.stream.ActorMaterializer
import akka.testkit.TestProbe
import java.util.UUID
import org.scalatest._
import repos.SecondaryIndex
import repos.jdbc.TableJanitor.State
import repos.testutils._
import scala.concurrent.ExecutionContext.Implicits.global

class SingleIndexCatchUpSpec extends org.scalatest.fixture.WordSpec with MustMatchers with LoneElement with Inside {
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
    val h2 = makeH2DB()
    val db = makeH2JdbcDb(h2)

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
      populateData1(db)
    }
  }

  override def withFixture(test: OneArgTest) = {
    val t = new CatchUpTest
    try {
      test(t)
    } finally {
      t.materializer.shutdown()
      await(t.system.terminate())
      await(t.h2.shutdown)
    }
  }

  "TableJanitor" must {
    "be able to catch up an index from scratch" in {
      t =>
        import t._
        t.createTable()
        val statusTable = Map.empty[String, Long].withDefaultValue(-1L)

        val r = TableJanitor.catchUpForRepo(db, db, System.currentTimeMillis(), statusTable, FooRepo,
          State(0, 0, Vector.empty))
        val newStatus = TableJanitor.loadJanitorIndexStatus(db)
        val nameOfFullTable = FooRepo.name
        val nameOfIndex =
          db.asInstanceOf[JdbcDb].innerIndex(
            SecondaryIndex(FooRepo,FooRepo.firstAtLeastTwoIndex.name,FooRepo.firstAtLeastTwoIndex.projection)
          ).ix3TableName
        val countOfFullTable = tableSize(db.asInstanceOf[JdbcDb], nameOfFullTable)
        val countOfIndexTable = tableSize(db.asInstanceOf[JdbcDb], nameOfIndex)
        countOfIndexTable must be < countOfFullTable
    }
  }
}

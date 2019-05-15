package repos

import java.util.UUID

import org.scalatest.{MustMatchers, OptionValues}
import repos.inmem.InMemDb
import repos.testutils.{FooRepo, FooId, TestUtils}
import scala.concurrent.ExecutionContext.Implicits.global

import TestUtils.await
import TestUtils.awaitStream


class IndexSpec extends org.scalatest.fixture.FlatSpec with MustMatchers with OptionValues {
  type FixtureParam = Database

  // Runs each tests against JDbcDb and InMemDb
  def withFixture(test: OneArgTest) = {
    val jdb = TestUtils.makeH2DB()
    val JdbcDb = TestUtils.makeH2JdbcDb(jdb)

    object InMemDb extends InMemDb

    await(JdbcDb.run(FooRepo.create()))
    await(InMemDb.run(FooRepo.create()))
    try {
      val o = test(JdbcDb)
      if (!o.isSucceeded) o
      else test(InMemDb)
    } finally {
      jdb.shutdown
    }
  }

  "partial indexes" should "work" in {
    db =>
      val id1 = FooId(UUID.randomUUID())
      val id2 = FooId(UUID.randomUUID())
      val id3 = FooId(UUID.randomUUID())
      val id4 = FooId(UUID.randomUUID())
      val id5 = FooId(UUID.randomUUID())
      await(db.run(FooRepo.insert(id1, "8")))
      await(db.run(FooRepo.insert(id2, "14")))
      await(db.run(FooRepo.insert(id3, "275")))
      await(db.run(FooRepo.insert(id4, "35")))
      await(db.run(FooRepo.insert(id5, "ababa")))
      await(db.run(FooRepo.lengthIndex.allMatching(0))) must be(Seq())
  }
}
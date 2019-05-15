package repos

import java.util.UUID

import org.scalatest.{MustMatchers, OptionValues}
import repos.testutils.{FooId, FooRepo, TestUtils}

import scala.concurrent.ExecutionContext.Implicits.global
import TestUtils.await
import repos.jdbc.JdbcDb
import slick.model.QualifiedName

import scala.concurrent.Future


class IndexSpec extends org.scalatest.fixture.FlatSpec with MustMatchers with OptionValues {
  type FixtureParam = Database

  def withFixture(test: OneArgTest) = {
    val jdb = TestUtils.makeH2DB()
    val JdbcDb = TestUtils.makeH2JdbcDb(jdb)

    await(JdbcDb.run(FooRepo.create()))
    try {
      test(JdbcDb)
    } finally {
      jdb.shutdown
    }
  }
  class FooTable(tableName:String) extends slick.model.Table(QualifiedName(tableName), Seq(), None, Seq(), Seq(), Set()) {
  }

  // TODO: remove blocking
  def sizeOfTable(db:JdbcDb, nameOfTable:String) : Int = {
    {
      val s = db.asInstanceOf[JdbcDb].db.source.createConnection().createStatement()
      val sql = s"select count(*) as a from ${nameOfTable}"
      s.execute( sql)
      val rs = s.getResultSet()
      rs.next()
      val num = rs.getInt("a")
      // useful for debugging:
      //println(s"sql=${sql}\ncount=${num}")
      num
    }
  }

  "partial indexes" should "have records cleaned up" in {
    db =>
      val id1 = FooId(UUID.randomUUID())
      val id2 = FooId(UUID.randomUUID())
      val id3 = FooId(UUID.randomUUID())
      val id4 = FooId(UUID.randomUUID())
      val id5 = FooId(UUID.randomUUID())
      val id6 = FooId(UUID.randomUUID())
      await(db.run(FooRepo.insert(id1, "8")))
      await(db.run(FooRepo.insert(id2, "14")))
      await(db.run(FooRepo.insert(id3, "275")))
      await(db.run(FooRepo.insert(id4, "35")))
      await(db.run(FooRepo.insert(id5, "ababa")))
      await(db.run(FooRepo.insert(id6, "")))

      val nameOfFullTable = FooRepo.name
      val nameOfIndex =
        db.asInstanceOf[JdbcDb].innerIndex(
          SecondaryIndex(FooRepo,FooRepo.firstAtLeastTwoIndex.name,FooRepo.firstAtLeastTwoIndex.projection)
        ).ix3TableName
      val countOfFullTable = sizeOfTable(db.asInstanceOf[JdbcDb], nameOfFullTable)
      val countOfIndexTable = sizeOfTable(db.asInstanceOf[JdbcDb], nameOfIndex)
      countOfIndexTable must be < countOfFullTable
  }
}
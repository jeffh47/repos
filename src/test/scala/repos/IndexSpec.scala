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

  "Partial index" should "lack records for un-matched inputs" in {
    db =>
      IndexUtil.populateData1(db)
      val nameOfFullTable = FooRepo.name
      val nameOfIndex =
        db.asInstanceOf[JdbcDb].innerIndex(
          SecondaryIndex(FooRepo,FooRepo.firstAtLeastTwoIndex.name,FooRepo.firstAtLeastTwoIndex.projection)
        ).ix3TableName
      val countOfFullTable = IndexUtil.sizeOfTable(db.asInstanceOf[JdbcDb], nameOfFullTable)
      val countOfIndexTable = IndexUtil.sizeOfTable(db.asInstanceOf[JdbcDb], nameOfIndex)
      countOfIndexTable must be < countOfFullTable
  }
}
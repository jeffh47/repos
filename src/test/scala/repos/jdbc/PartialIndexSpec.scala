package repos.jdbc

import org.scalatest._
import repos.testutils.TestUtils.await
import repos.testutils._
import repos.{ Database, SecondaryIndex }
import scala.concurrent.ExecutionContext.Implicits.global
import slick.model.QualifiedName


class PartialIndexSpec extends org.scalatest.fixture.FlatSpec with MustMatchers with OptionValues {
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
      DbDataUtils.populateData1(db)
      val nameOfFullTable = FooRepo.name
      val nameOfIndex =
        db.asInstanceOf[JdbcDb].innerIndex(
          SecondaryIndex(FooRepo,FooRepo.firstAtLeastTwoIndex.name,FooRepo.firstAtLeastTwoIndex.projection)
        ).ix3TableName
      val countOfFullTable = DbDataUtils.sizeOfTable(db.asInstanceOf[JdbcDb], nameOfFullTable)
      val countOfIndexTable = DbDataUtils.sizeOfTable(db.asInstanceOf[JdbcDb], nameOfIndex)
      countOfIndexTable must be < countOfFullTable
  }
}
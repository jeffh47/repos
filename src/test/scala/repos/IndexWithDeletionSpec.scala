package repos

import java.util.UUID
import org.scalatest._
import repos.jdbc.JdbcDb
import repos.testutils.TestUtils._
import repos.testutils._
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

class IndexWithDeletionSpec extends fixture.FlatSpec with MustMatchers {
  type FixtureParam = Database

  def withFixture(test: OneArgTest): Outcome = {
    val jdb = makeH2DB
    val JdbcDb = makeH2JdbcDb(jdb)
    await(JdbcDb.run(FooRepo.create))
    try test(JdbcDb) finally jdb.shutdown
  }

  private def populateData(db: Database)(implicit ec:ExecutionContext): Unit = {
    val id1 = FooId(UUID.randomUUID)
    val id2 = FooId(UUID.randomUUID)
    val id3 = FooId(UUID.randomUUID)
    val id4 = FooId(UUID.randomUUID)
    val id5 = FooId(UUID.randomUUID)
    val id6 = FooId(UUID.randomUUID)

    await(db.run(FooRepo.insert(id1, "8")))
    await(db.run(FooRepo.insert(id2, "14")))
    await(db.run(FooRepo.insert(id3, "275")))
    await(db.run(FooRepo.insert(id4, "35")))
    await(db.run(FooRepo.insert(id5, "ababa")))
    await(db.run(FooRepo.insert(id6, "")))

    await(db.run(FooRepo.insert(id2, "1")))
  }

  "Index table" should "delete records before inserting" in { fixture =>
    val db = fixture.asInstanceOf[JdbcDb]
    populateData(db)
    val nameOfFullTable   = FooRepo.name
    val nameOfLatestTable = FooRepo.name + "_latest"
    val nameOfIndexTable = db.innerIndex(
      SecondaryIndex(FooRepo, FooRepo.firstAtLeastTwoIndex.name, FooRepo.firstAtLeastTwoIndex.projection)
    ).ix3TableName
    val countOfFullTable   = sizeOfTable(db, nameOfFullTable)
    val countOfLatestTable = sizeOfTable(db, nameOfLatestTable)
    val countOfIndexTable  = sizeOfTable(db, nameOfIndexTable)
    println(contents(db, nameOfIndexTable).mkString("\n"))
    countOfFullTable   must be (7)
    countOfLatestTable must be (6)
    countOfIndexTable  must be (3)
    //TODO: current delete logic makes the index table reflect _latest, not the full historical
  }

  private def contents(db: JdbcDb, nameOfTable: String) = {
    val s = db.db.source.createConnection.createStatement
    s.execute(s"select id, value from $nameOfTable")
    val r = mutable.ListBuffer[(String, String)]()
    val rs = s.getResultSet
    while(rs.next())
      r += (rs.getString("id") -> rs.getString("value"))
    r
  }

  private def sizeOfTable(db: JdbcDb, nameOfTable: String): Int = {
    val s = db.db.source.createConnection.createStatement
    s.execute(s"select count(*) as a from $nameOfTable")
    val rs = s.getResultSet
    rs.next()
    rs.getInt("a")
  }
}

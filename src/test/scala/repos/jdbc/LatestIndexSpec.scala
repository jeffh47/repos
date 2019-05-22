package repos.jdbc

import org.scalatest._
import repos.testutils.DbDataUtils._
import repos.testutils.TestUtils._
import repos.testutils._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global

class LatestIndexSpec extends WordSpec with MustMatchers {

  "Latest Index table" must {

    "delete records before inserting with JDBC DB" in {
      val jdb = makeH2DB
      val db = makeH2JdbcDb(jdb)
      await(db.run(FooRepo.create))
      populateData2(db)
      val nameOfFullTable   = FooRepo.name
      val nameOfLatestTable = FooRepo.name + "_latest"
      val nameOfFullIndexTable   = db.innerIndex(FooRepo.firstAtLeastTwoIndex).ix3TableName
      val nameOfLatestIndexTable = db.innerIndex(FooRepo.firstAtLeastTwoLatestIndex).ix3TableName
      println(  "Full table\n" + contents(db, nameOfFullTable,      "uuid", "entry_bin").mkString("\n"))
      println("Latest table\n" + contents(db, nameOfLatestTable,      "id", "entry_bin").mkString("\n"))
      println(  "Full index\n" + contents(db, nameOfFullIndexTable,   "id", "value"    ).mkString("\n"))
      println("Latest index\n" + contents(db, nameOfLatestIndexTable, "id", "value"    ).mkString("\n"))
      sizeOfTable(db, nameOfFullTable)        must be (7)
      sizeOfTable(db, nameOfLatestTable)      must be (6)
      sizeOfTable(db, nameOfFullIndexTable)   must be (4)
      sizeOfTable(db, nameOfLatestIndexTable) must be (3)
      await(jdb.shutdown)
    }
  }

  private def contents(db: JdbcDb, nameOfTable: String, keyCol: String, valueCol: String) = {
    val s = db.db.source.createConnection.createStatement
    s.execute(s"select $keyCol, $valueCol from $nameOfTable")
    val r = mutable.ListBuffer[(String, String)]()
    val rs = s.getResultSet
    while(rs.next())
      r += (rs.getString(keyCol) -> rs.getString(valueCol)) //todo decode with JdbcDb.parse if encoded
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

package repos.jdbc

import org.scalatest._
import repos.testutils._
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
      println(  "Full table\n" + tableContents(db, nameOfFullTable,      "uuid", "entry_bin").mkString("\n"))
      println("Latest table\n" + tableContents(db, nameOfLatestTable,      "id", "entry_bin").mkString("\n"))
      println(  "Full index\n" + tableContents(db, nameOfFullIndexTable,   "id", "value"    ).mkString("\n"))
      println("Latest index\n" + tableContents(db, nameOfLatestIndexTable, "id", "value"    ).mkString("\n"))
      tableSize(db, nameOfFullTable)        must be (7)
      tableSize(db, nameOfLatestTable)      must be (6)
      tableSize(db, nameOfFullIndexTable)   must be (4)
      tableSize(db, nameOfLatestIndexTable) must be (3)
      await(jdb.shutdown)
    }
  }
}

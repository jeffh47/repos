package repos.inmem

import org.scalatest._
import repos.testutils._
import scala.concurrent.ExecutionContext.Implicits.global

class LatestIndexSpec extends WordSpec with MustMatchers {

  "Latest Index table" must {

    "delete records before inserting with in-memory DB" in {
      val db = new InMemDb
      await(db.run(FooRepo.create))
      populateData2(db)
      await(db.run(FooRepo.getEntries())).size     must be (7)
      await(db.run(FooRepo.allLatestEntries)).size must be (6)
      await(db.run(FooRepo.firstAtLeastTwoIndex.tableSize)) must be (4)
      await(db.run(FooRepo.firstAtLeastTwoLatestIndex.tableSize)) must be (3)
    }
  }
}

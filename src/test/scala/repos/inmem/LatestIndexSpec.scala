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
      db.repoMap("foo").indexMap("first_at_least_two_ch").data.size        must be (4)
      db.repoMap("foo").indexMap("first_at_least_two_ch_latest").data.size must be (3)
    }
  }
}

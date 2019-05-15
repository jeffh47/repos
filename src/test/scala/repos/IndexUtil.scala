package repos

import java.util.UUID

import repos.jdbc.JdbcDb
import repos.testutils.{FooId, FooRepo}
import repos.testutils.TestUtils.await

import scala.concurrent.ExecutionContext

object IndexUtil {
  def populateData1(db:Database)(implicit ec:ExecutionContext) = {
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

    // mutate so that an object included in the partialIndexTable becomes excluded from the partialIndexTable
    await(db.run(FooRepo.delete(Set(id2))))
    await(db.run(FooRepo.insert(id2, "1")))
    // See also: .insertWithoutLatest
  }
}

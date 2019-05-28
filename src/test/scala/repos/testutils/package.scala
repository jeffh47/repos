package repos

import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration

package object testutils {

  def await[R](f: => Future[R]): R = Await.result(f, Duration.Inf)

  def awaitStream[T](publisher: RepoPublisher[T]): ArrayBuffer[T] = {
    val b = ArrayBuffer.empty[T]
    await(publisher.foreach(s => synchronized { b+=s }))
    b.result()
  }

  def populateData1(db:Database)(implicit ec: ExecutionContext): Unit = {
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

  def populateData2(db: Database)(implicit ec: ExecutionContext): Unit = {
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

    await(db.run(FooRepo.insert(id2, "1"))) // Change valid to invalid. Appends in full table, updates in latest table
  }
}

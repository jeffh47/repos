package repos

import scala.collection.mutable
import scala.util.Random
import slick.jdbc.JdbcBackend
import slick.jdbc.JdbcBackend.Database

package object jdbc {

  def makeH2DB(): JdbcBackend.DatabaseDef = {
    Database.forURL(
      s"jdbc:h2:mem:test_${Random.nextInt};DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1",
      driver = "org.h2.Driver")
  }

  def makeH2JdbcDb(jdb: JdbcBackend.DatabaseDef): JdbcDb = {
    new JdbcDb(slick.driver.H2Driver, jdb)
  }

  def tableContents(db: JdbcDb, tableName: String, keyCol: String, valueCol: String): Seq[(String, String)] = {
    val s = db.db.source.createConnection.createStatement
    s.execute(s"select $keyCol, $valueCol from $tableName")
    val r = mutable.ListBuffer[(String, String)]()
    val rs = s.getResultSet
    while(rs.next())
      r += (rs.getString(keyCol) -> rs.getString(valueCol))
    r
  }

  def tableSize(db: JdbcDb, tableName: String): Int = {
    val s = db.db.source.createConnection.createStatement
    s.execute(s"select count(*) as a from $tableName")
    val rs = s.getResultSet
    rs.next()
    rs.getInt("a")
  }
}

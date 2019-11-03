package io.flow.postgresql

import java.util.UUID

import anorm.SqlParser
import io.flow.postgresql.db.TestDatabase
import io.flow.util.Random
import org.scalatest.{FunSpec, Matchers}

import scala.util.{Failure, Success, Try}

class QueryBatchSpec extends FunSpec with Matchers {

  private[this] val SelectUserQuery = Query("select name from test_users")

  private[this] val UpsertUserQuery = Query(
    """
      |insert into test_users
      | (id, name)
      |values
      | ({id}, {name})
      |on conflict(id) do update
      | set name = {name}
      |""".stripMargin
  )

  case class User(id: String, name: String)

  private[this] val random = Random()
  def randomString(): String = random.alphaNumeric(36)

  def findUserById(id: String): Option[User] = {
    TestDatabase.withConnection { implicit c =>
      SelectUserQuery
        .equals("id", id)
        .as(SqlParser.str("name").*)
        .headOption.map { n =>
        User(id = id, name = n)
      }
    }
  }

  def createUser(name: String): User = {
    val id = UUID.randomUUID().toString
    TestDatabase.withConnection { implicit c =>
      UpsertUserQuery
        .bind("id", id)
        .bind("name", name)
        .anormSql().execute()
    }
    findUserById(id).get
  }

  it("create single user") {
    val user1 = createUser(name = "A")
    val user2 = createUser(name = "B")
    findUserById(user1.id).get.name should be("A")
    findUserById(user2.id).get.name should be("B")
  }

  it("BatchQueryBuilder") {
    val name1 = randomString()
    val name2 = randomString()

    TestDatabase.withConnection { implicit c =>
      BatchQueryBuilder(UpsertUserQuery.withDebugging())
        .withRow { r => r.bind("id", "1").bind("name", name1) }
        .withRow { r => r.bind("id", "2").bind("name", name2) }
        .build()
        .execute()
    }

    findUserById("1").get.name should be(name1)
    findUserById("2").get.name should be(name2)
  }

  it("BatchQueryBuilder throws error if no rows") {
    Try {
      TestDatabase.withConnection { implicit c =>
        BatchQueryBuilder(UpsertUserQuery.withDebugging())
          .build()
          .execute()
      }
    } match {
      case Success(_) => sys.error("Expected error")
      case Failure(ex) => ex.getMessage.contains("Must have at least one row to execute a batch sql query") should be(true)
    }
  }


  it("BatchQueryBuilder.execute") {
    val name1 = randomString()
    val name2 = randomString()

    TestDatabase.withConnection { implicit c =>
      BatchQueryBuilder(UpsertUserQuery.withDebugging())
        .withRow { r => r.bind("id", "1").bind("name", name1) }
        .withRow { r => r.bind("id", "2").bind("name", name2) }
        .execute
    }

    findUserById("1").get.name should be(name1)
    findUserById("2").get.name should be(name2)
  }

  it("BatchQueryBuilder.execute is a no op with empty rows") {
    TestDatabase.withConnection { implicit c =>
      BatchQueryBuilder(UpsertUserQuery).execute
    } should be(Array.empty)
  }
  /*

  it("create users in batch") {
    val ids = 0.to(3).map { _ => UUID.randomUUID().toString }
    val batch = ids.map { id =>
      UpsertUserQuery.
        .bind("id", id)
        .bind("name", UUID.randomUUID().toString)
      .
    }
    TestDatabase.withConnection { implicit c =>
    }
  }

   */
}

package io.flow.postgresql

import java.util.UUID

import anorm.SqlParser
import io.flow.postgresql.db.TestDatabase
import org.scalatest.{FunSpec, Matchers}

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

  it("create users in batch") {
    val user1 = createUser(name = "A")
    val user2 = createUser(name = "B")
    println(s"user[${user1}] --  $user2")
  }

}

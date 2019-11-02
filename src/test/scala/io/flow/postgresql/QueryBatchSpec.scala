package io.flow.postgresql

import java.util.UUID

import io.flow.postgresql.db.TestDatabase
import org.scalatest.{FunSpec, Matchers}

class QueryBatchSpec extends FunSpec with Matchers {

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

  def createUser(name: String): User = {
    val id = UUID.randomUUID().toString
    TestDatabase.withConnection { implicit c =>
      UpsertUserQuery.withDebugging()
        .bind("id", id)
        .bind("name", name)
        .anormSql().execute()
    }
    User(
      id = id,
      name = name,
    )
  }

  it("batch_upsert") {
    val user1 = createUser(name = "A")
    val user2 = createUser(name = "B")
    println(s"user[${user1}] --  $user2")
  }

}

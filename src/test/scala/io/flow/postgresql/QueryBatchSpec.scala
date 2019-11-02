package io.flow.postgresql

import play.api.db.Database
import io.flow.test.utils.FlowPlaySpec

class QueryBatchSpec extends FlowPlaySpec {

  private[this] val db: Database = init[Database]

  private[this] val UpsertUserQuery = Query(
    """
      |insert into test_users
      |(id, name)
      |values
      |({id}, {name})
      |on conflict(id)
      |set name = {name}
      |""".stripMargin
  )
  case class User(id: Long, name: String)

  def createUser(id: Long, name: String): User = {
    db.withConnection { implicit c =>
      UpsertUserQuery
        .bind("id", id)
        .bind("name", name)
        .anormSql().execute()
    }
    User(
      id = id,
      name = name,
    )
  }

  "batch_upsert" in {
    val user1 = createUser(id = 1, name = "A")
    val user2 = createUser(id = 2, name = "B")
    println(s"user[${user1}] --  $user2")
  }

}

package io.flow.play.postgresql

import scala.util.{Failure, Success, Try}
import java.util.UUID
import org.scalatest.{FunSpec, Matchers}

class QuerySpec extends FunSpec with Matchers {

  def validate(query: Query, sql: String, interpolate: String) {
    query.sql should be(sql)
    query.interpolate should be(interpolate)
  }

  it("base") {
    Query("select 1").interpolate should be("select 1")
  }

  it("multi") {
    validate(
      Query("select * from users").multi("email", None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").multi("email", Some(Nil)),
      "select * from users where false",
      "select * from users where false"
    )

    validate(
      Query("select * from users").multi("email", Some(Seq("mike@flow.io"))),
      "select * from users where email in ({email})",
      "select * from users where email in ('mike@flow.io')"
    )

    validate(
      Query("select * from users").multi("email", Some(Seq("mike@flow.io", "paolo@flow.io"))),
      "select * from users where email in ({email}, {email2})",
      "select * from users where email in ('mike@flow.io', 'paolo@flow.io')"
    )

    validate(
      Query("select * from users").multi("users.email", Some(Seq("mike@flow.io", "paolo@flow.io"))),
      "select * from users where users.email in ({email}, {email2})",
      "select * from users where users.email in ('mike@flow.io', 'paolo@flow.io')"
    )
  }

  it("number") {
    Query("select * from users").number("age", None).interpolate should be("select * from users")
    Query("select * from users").number("age", Some(5)).sql should be("select * from users where age = {age}")
    Query("select * from users").number("age", Some(5)).interpolate should be("select * from users where age = 5")
  }

  it("uuid") {
    val guid = UUID.randomUUID
    Query("select * from users").uuid("users.guid", None).interpolate should be(
      "select * from users"
    )

    Query("select * from users").uuid("users.guid", Some(guid)).sql should be(
      "select * from users where users.guid = {guid}::uuid"
    )

    Query("select * from users").uuid("users.guid", Some(guid)).interpolate should be(
      s"select * from users where users.guid = '$guid'::uuid"
    )
  }

  it("text") {
    Query("select * from users").text("users.email", None).interpolate should be("select * from users")
    Query("select * from users").text("users.email", Some("mike@flow.io")).sql should be(
      "select * from users where users.email = trim({email})"
    )
    Query("select * from users").text("users.email", Some("mike@flow.io")).interpolate should be(
      "select * from users where users.email = trim('mike@flow.io')"
    )

    Query("select * from users").
      text(
      "users.email",
      Some("mike@flow.io"),
      columnFunctions = Seq(Query.Function.Lower),
      valueFunctions = Seq(Query.Function.Trim, Query.Function.Lower)
      ).sql should be(
        "select * from users where lower(users.email) = trim(lower({email}))"
      )
  }

  it("text with same variable twice") {
    Query("select * from users").
      text("users.email", Some("mike@flow.io")).
      text("EMAIL", Some("paolo@flow.io")).
      sql should be(
        "select * from users where users.email = trim({email}) and EMAIL = trim({email2})"
      )
  }

  it("condition") {
    Query("select * from users").condition(None).interpolate should be("select * from users")
    Query("select * from users").condition(Some("email is not null")).sql should be(
      "select * from users where email is not null"
    )
  }

  it("bind") {
    Query("select * from users").bind("test", None).interpolate should be("select * from users")

    val query = Query("select * from users").
      condition(Some("email = {email}")).
      bind("email", Some("mike@flow.io"))

    query.sql should be("select * from users where email = {email}")
    query.interpolate should be("select * from users where email = 'mike@flow.io'")
  }

  it("bind with duplicate variable name raises an error") {
    Try {
      Query("select * from users").
        text("users.email", Some("mike@flow.io")).
        bind("email", Some("paolo@flow.io"))
    } match {
      case Success(_) => fail("Expected error for duplicate bind variable")
      case Failure(ex) => {
        ex.getMessage should be("Bind variable named 'email' already defined")
      }
    }
  }

  it("boolean") {
    Query("select * from users").boolean("users.verified", None).interpolate should be(
      "select * from users"
    )
    Query("select * from users").boolean("users.verified", Some(true)).sql should be(
      "select * from users where users.verified is true"
    )
    Query("select * from users").boolean("users.verified", Some(false)).sql should be(
      "select * from users where users.verified is false"
    )
  }

  it("nullBoolean") {
    Query("select * from users").nullBoolean("users.deleted_at", None).interpolate should be(
      "select * from users"
    )
    Query("select * from users").nullBoolean("users.deleted_at", Some(true)).sql should be(
      "select * from users where users.deleted_at is not null"
    )
    Query("select * from users").nullBoolean("users.deleted_at", Some(false)).sql should be(
      "select * from users where users.deleted_at is null"
    )
  }

  it("isNull") {
    Query("select * from users").isNull("users.email").sql should be(
      "select * from users where users.email is null"
    )
  }

  it("isNotNull") {
    Query("select * from users").isNotNull("users.email").sql should be(
      "select * from users where users.email is not null"
    )
  }

  it("subquery") {
    Query("select * from users").
      subquery("users.id", "group_id", None, { bindVar =>
        s"select user_id from memberships where group_id = {$bindVar}"
      }).
      sql should be(
        "select * from users"
      )

    Query("select * from users").
      subquery("users.id", "group_id", Some(5), { bindVar =>
        s"select user_id from memberships where group_id = {$bindVar}"
      }).
      sql should be(
        "select * from users where users.id in (select user_id from memberships where group_id = {group_id})"
      )
  }

  it("orderBy") {
    Query("select * from users").
      orderBy(None).
      sql should be(
        "select * from users"
      )

    Query("select * from users").
      orderBy(Some("users.created_at")).
      sql should be(
        "select * from users order by users.created_at"
      )

    Query("select * from users").
      orderBy(Some("lower(email)")).
      orderBy(Some("users.created_at")).
      sql should be(
        "select * from users order by lower(email), users.created_at"
      )
  }

  it("limit") {
    Query("select * from users").limit(None).sql should be(
      "select * from users"
    )

    Query("select * from users").limit(Some(5)).sql should be(
      "select * from users limit 5"
    )
  }

  it("offset") {
    Query("select * from users").offset(None).sql should be(
      "select * from users"
    )

    Query("select * from users").offset(Some(5)).sql should be(
      "select * from users offset 5"
    )
  }

}

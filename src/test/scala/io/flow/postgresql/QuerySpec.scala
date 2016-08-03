package io.flow.postgresql

import scala.util.{Failure, Success, Try}
import java.util.UUID
import org.scalatest.{FunSpec, Matchers}

class QuerySpec extends FunSpec with Matchers {

  def validate(query: Query, sql: String, interpolate: String) {
    query.sql == sql match {
      case true => {
        // No-op
      }
      case false => {
        println("expected: " + sql)
        println("  actual: " + query.sql)
        query.sql should be(sql)
      }
    }

    query.interpolate == interpolate match {
      case true => {
        // No-op
      }
      case false => {
        println("expected: " + interpolate)
        println("  actual: " + query.interpolate)
        query.interpolate should be(interpolate)
      }
    }
  }

  it("base") {
    validate(
      Query("select 1"),
      "select 1",
      "select 1"
    )
  }

  it("equals") {
    validate(
      Query("select * from users").equals("id", Some(5)),
      "select * from users where id = {id}::numeric",
      "select * from users where id = 5"
    )
  }

  it("notEquals") {
    validate(
      Query("select * from users").notEquals("id", Some(5)),
      "select * from users where id != {id}::numeric",
      "select * from users where id != 5"
    )
  }

  it("lessThan") {
    validate(
      Query("select * from users").lessThan("id", Some(5)),
      "select * from users where id < {id}::numeric",
      "select * from users where id < 5"
    )
  }

  it("lessThanOrEquals") {
    validate(
      Query("select * from users").lessThanOrEquals("id", Some(5)),
      "select * from users where id <= {id}::numeric",
      "select * from users where id <= 5"
    )
  }

  it("greaterThan") {
    validate(
      Query("select * from users").greaterThan("id", Some(5)),
      "select * from users where id > {id}::numeric",
      "select * from users where id > 5"
    )
  }

  it("greaterThanOrEquals") {
    validate(
      Query("select * from users").greaterThanOrEquals("id", Some(5)),
      "select * from users where id >= {id}::numeric",
      "select * from users where id >= 5"
    )
  }

  it("in") {
    validate(
      Query("select * from users").optionalIn("email", None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").optionalIn("email", Some(Nil)),
      "select * from users where false",
      "select * from users where false"
    )

    validate(
      Query("select * from users").in("email", Seq("mike@flow.io")),
      "select * from users where email in (trim({email}))",
      "select * from users where email in (trim('mike@flow.io'))"
    )

    validate(
      Query("select * from users").in("email", Seq("mike@flow.io", "paolo@flow.io")),
      "select * from users where email in (trim({email}), trim({email2}))",
      "select * from users where email in (trim('mike@flow.io'), trim('paolo@flow.io'))"
    )

    validate(
      Query("select * from users").in("users.email", Seq("mike@flow.io", "paolo@flow.io")),
      "select * from users where users.email in (trim({email}), trim({email2}))",
      "select * from users where users.email in (trim('mike@flow.io'), trim('paolo@flow.io'))"
    )

    validate(
      Query("select * from users").in("table.json->>'jsonfield'", Seq("jsonvalue")),
      "select * from users where table.json->>'jsonfield' in (trim({json_jsonfield}))",
      "select * from users where table.json->>'jsonfield' in (trim('jsonvalue'))"
    )
  }

  it("in with uuid") {
    val guids = Seq(UUID.randomUUID, UUID.randomUUID)
    validate(
      Query("select * from users").in("guid", guids),
      "select * from users where guid in ({guid}::uuid, {guid2}::uuid)",
      "select * from users where guid in " + guids.mkString("('", "'::uuid, '", "'::uuid)")
    )
  }

  it("in with functions") {
    val ids = Seq("a", "b")
    validate(
      Query("select * from users").in(
        "id",
        ids,
        columnFunctions = Seq(Query.Function.Lower),
        valueFunctions = Seq(Query.Function.Lower, Query.Function.Trim)
      ),
      "select * from users where lower(id) in (lower(trim({id})), lower(trim({id2})))",
      "select * from users where lower(id) in (lower(trim('a')), lower(trim('b')))"
    )
  }

  it("numbers") {
    validate(
      Query("select * from users").equals("age", None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").equals("age", Some(5)),
      "select * from users where age = {age}::numeric",
      "select * from users where age = 5"
    )
  }

  it("uuid") {
    val guid = UUID.randomUUID
    validate(
      Query("select * from users").equals("users.guid", None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").equals("users.guid", Some(guid)),
      "select * from users where users.guid = {guid}::uuid",
      s"select * from users where users.guid = '$guid'::uuid"
    )
  }

  it("text") {
    validate(
      Query("select * from users").optionalText("users.email", None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").optionalText("users.email", Some("mike@flow.io")),
      "select * from users where users.email = trim({email})",
      "select * from users where users.email = trim('mike@flow.io')"
    )

    validate(
      Query("select * from users").
        text(
          "users.email",
          "mike@flow.io",
          columnFunctions = Seq(Query.Function.Lower),
          valueFunctions = Seq(Query.Function.Trim, Query.Function.Lower)
        ),
      "select * from users where lower(users.email) = trim(lower({email}))",
      "select * from users where lower(users.email) = trim(lower('mike@flow.io'))"
    )
  }

  it("text with same variable twice") {
    validate(
      Query("select * from users").
        text("users.email", "mike@flow.io").
        text("EMAIL", "paolo@flow.io"),
      "select * from users where users.email = trim({email}) and EMAIL = trim({email2})",
      "select * from users where users.email = trim('mike@flow.io') and EMAIL = trim('paolo@flow.io')"
    )
  }

  it("and") {
    validate(
      Query("select * from users").and(None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").and(Some("email is not null")),
      "select * from users where email is not null",
      "select * from users where email is not null"
    )

    validate(
      Query("select * from users").and(Seq("email is not null", "name is not null")),
      "select * from users where email is not null and name is not null",
      "select * from users where email is not null and name is not null"
    )
  }

  it("or") {
    validate(
      Query("select * from users").or(None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").or(Some("email is not null")),
      "select * from users where email is not null",
      "select * from users where email is not null"
    )

    validate(
      Query("select * from users").or(Seq("email is not null", "name is not null")),
      "select * from users where (email is not null or name is not null)",
      "select * from users where (email is not null or name is not null)"
    )
  }

  it("bind") {
    validate(
      Query("select * from users").bind("test", None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").
        and("email = {email}").
        bind("email", Some("mike@flow.io")),
      "select * from users where email = {email}",
      "select * from users where email = 'mike@flow.io'"
    )
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
    validate(
      Query("select * from users").boolean("users.verified", None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").boolean("users.verified", Some(true)),
      "select * from users where users.verified is true",
      "select * from users where users.verified is true"
    )

    validate(
      Query("select * from users").boolean("users.verified", Some(false)),
      "select * from users where users.verified is false",
      "select * from users where users.verified is false"
    )
  }

  it("nullBoolean") {
    validate(
      Query("select * from users").nullBoolean("users.deleted_at", None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").nullBoolean("users.deleted_at", Some(true)),
      "select * from users where users.deleted_at is not null",
      "select * from users where users.deleted_at is not null"
    )

    validate(
      Query("select * from users").nullBoolean("users.deleted_at", Some(false)),
      "select * from users where users.deleted_at is null",
      "select * from users where users.deleted_at is null"
    )
  }

  it("isNull") {
    validate(
      Query("select * from users").isNull("users.email"),
      "select * from users where users.email is null",
      "select * from users where users.email is null"
    )
  }

  it("isNotNull") {
    validate(
      Query("select * from users").isNotNull("users.email"),
      "select * from users where users.email is not null",
      "select * from users where users.email is not null"
    )
  }

  it("groupBy and orderBy") {
    validate(
      Query("select * from users").
        orderBy(Some("users.y")).
        orderBy(Some("users.x")).
        groupBy(Some("users.id")).
        groupBy(Some("users.name")),
      "select * from users group by users.id, users.name order by users.y, users.x",
      "select * from users group by users.id, users.name order by users.y, users.x"
    )
  }

  it("groupBy") {
    validate(
      Query("select * from users").
        groupBy(None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").
        groupBy(Some("users.id")),
      "select * from users group by users.id",
      "select * from users group by users.id"
    )

    validate(
      Query("select * from users").
        groupBy(Some("users.id")).
        groupBy(Some("users.name")),
      "select * from users group by users.id, users.name",
      "select * from users group by users.id, users.name"
    )
  }

  it("orderBy") {
    validate(
      Query("select * from users").
        orderBy(None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").
        orderBy(Some("users.created_at")),
      "select * from users order by users.created_at",
      "select * from users order by users.created_at"
    )

    validate(
      Query("select * from users").
        orderBy(Some("lower(email)")).
        orderBy(Some("users.created_at")),
      "select * from users order by lower(email), users.created_at",
      "select * from users order by lower(email), users.created_at"
    )
  }

  it("limit") {
    validate(
      Query("select * from users").limit(5),
      "select * from users limit 5",
      "select * from users limit 5"
    )
  }

  it("offset") {
    validate(
      Query("select * from users").offset(5),
      "select * from users offset 5",
      "select * from users offset 5"
    )
  }

  it("json array") {
    validate(
      Query("select * from users").operation("table.json::jsonb", "?|", "value", valueFunctions = Seq(Query.Function.Custom("array"))),
      "select * from users where table.json::jsonb ?| array[trim({json_jsonb})]",
      "select * from users where table.json::jsonb ?| array[trim('value')]"
    )
  }

  it("debuggingInfo simple query") {
    Query("select * from users").debuggingInfo() should be("select * from users")
  }

  it("debuggingInfo query w/ bind vars") {
    val q = Query("select * from users").equals("id", Some(5))
    q.debuggingInfo() should be(
      Seq(
        "select * from users where id = {id}::numeric",
        " - id: 5",
        "Interpolated:",
        "select * from users where id = 5"
      ).mkString("\n")
    )
  }

}

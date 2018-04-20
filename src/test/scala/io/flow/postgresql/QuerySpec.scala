package io.flow.postgresql

import scala.util.{Failure, Success, Try}
import java.util.UUID

import org.joda.time.{DateTime, LocalDate}
import org.scalatest.{FunSpec, Matchers}

class QuerySpec extends FunSpec with Matchers {

  def validate(query: Query, sql: String, interpolate: String) {
    if (query.sql != sql) {
      println("expected: " + sql)
      println("  actual: " + query.sql)
      query.sql should be(sql)
    }

    if (query.interpolate != interpolate) {
      println("expected: " + interpolate)
      println("  actual: " + query.interpolate)
      query.interpolate should be(interpolate)
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
      "select * from users where id = {id}::int",
      "select * from users where id = 5"
    )
  }

  it("equalsIgnoreCase") {
    validate(
      Query("select * from users").equalsIgnoreCase("id", Some(5)),
      "select * from users where id = {id}::int",
      "select * from users where id = 5"
    )

    validate(
      Query("select * from users").equalsIgnoreCase("name", Some("flow")),
      "select * from users where lower(trim(name)) = lower(trim({name}))",
      "select * from users where lower(trim(name)) = lower(trim('flow'))"
    )
  }

  it("notEqualsIgnoreCase") {
    validate(
      Query("select * from users").notEqualsIgnoreCase("id", Some(5)),
      "select * from users where id != {id}::int",
      "select * from users where id != 5"
    )

    validate(
      Query("select * from users").notEqualsIgnoreCase("name", Some("flow")),
      "select * from users where lower(trim(name)) != lower(trim({name}))",
      "select * from users where lower(trim(name)) != lower(trim('flow'))"
    )
  }

  it("notEquals") {
    validate(
      Query("select * from users").notEquals("id", Some(5)),
      "select * from users where id != {id}::int",
      "select * from users where id != 5"
    )
  }

  it("lessThan") {
    validate(
      Query("select * from users").lessThan("id", Some(5)),
      "select * from users where id < {id}::int",
      "select * from users where id < 5"
    )
  }

  it("lessThanOrEquals") {
    validate(
      Query("select * from users").lessThanOrEquals("id", Some(5)),
      "select * from users where id <= {id}::int",
      "select * from users where id <= 5"
    )
  }

  it("greaterThan") {
    validate(
      Query("select * from users").greaterThan("id", Some(5)),
      "select * from users where id > {id}::int",
      "select * from users where id > 5"
    )
  }

  it("greaterThanOrEquals") {
    validate(
      Query("select * from users").greaterThanOrEquals("id", Some(5)),
      "select * from users where id >= {id}::int",
      "select * from users where id >= 5"
    )
  }

  it("equals w/ date") {
    val date = LocalDate.now
    validate(
      Query("select * from users").equals("created_at", Some(date)),
      "select * from users where created_at = {created_at}::date",
      s"select * from users where created_at = '$date'::date"
    )
  }

  it("lessThan w/ date") {
    val date = LocalDate.now
    validate(
      Query("select * from users").lessThan("created_at", Some(date)),
      "select * from users where created_at < {created_at}::date",
      s"select * from users where created_at < '$date'::date"
    )
  }

  it("lessThanOrEquals w/ date") {
    val date = LocalDate.now
    validate(
      Query("select * from users").lessThanOrEquals("created_at", Some(date)),
      "select * from users where created_at <= {created_at}::date",
      s"select * from users where created_at <= '$date'::date"
    )
  }

  it("greaterThan w/ date") {
    val date = LocalDate.now
    validate(
      Query("select * from users").greaterThan("created_at", Some(date)),
      "select * from users where created_at > {created_at}::date",
      s"select * from users where created_at > '$date'::date"
    )
  }

  it("greaterThanOrEquals w/ date") {
    val date = LocalDate.now
    validate(
      Query("select * from users").greaterThanOrEquals("created_at", Some(date)),
      "select * from users where created_at >= {created_at}::date",
      s"select * from users where created_at >= '$date'::date"
    )
  }

  it("lessThan w/ date time") {
    val ts = DateTime.now
    validate(
      Query("select * from users").lessThan("created_at", Some(ts)),
      "select * from users where created_at < {created_at}::timestamptz",
      s"select * from users where created_at < '$ts'::timestamptz"
    )
  }

  it("lessThanOrEquals w/ date time") {
    val ts = DateTime.now
    validate(
      Query("select * from users").lessThanOrEquals("created_at", Some(ts)),
      "select * from users where created_at <= {created_at}::timestamptz",
      s"select * from users where created_at <= '$ts'::timestamptz"
    )
  }

  it("greaterThan w/ date time") {
    val ts = DateTime.now
    validate(
      Query("select * from users").greaterThan("created_at", Some(ts)),
      "select * from users where created_at > {created_at}::timestamptz",
      s"select * from users where created_at > '$ts'::timestamptz"
    )
  }

  it("greaterThanOrEquals w/ date time") {
    val ts = DateTime.now
    validate(
      Query("select * from users").greaterThanOrEquals("created_at", Some(ts)),
      "select * from users where created_at >= {created_at}::timestamptz",
      s"select * from users where created_at >= '$ts'::timestamptz"
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

  it("not in") {
    validate(
      Query("select * from users").optionalNotIn("email", None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").optionalNotIn("email", Some(Nil)),
      "select * from users where false",
      "select * from users where false"
    )

    validate(
      Query("select * from users").notIn("email", Seq("mike@flow.io")),
      "select * from users where email not in (trim({email}))",
      "select * from users where email not in (trim('mike@flow.io'))"
    )

    validate(
      Query("select * from users").notIn("email", Seq("mike@flow.io", "paolo@flow.io")),
      "select * from users where email not in (trim({email}), trim({email2}))",
      "select * from users where email not in (trim('mike@flow.io'), trim('paolo@flow.io'))"
    )
  }

  it("in with datetime") {
    val ts = DateTime.now
    val values = Seq(ts, ts.plusHours(1))
    validate(
      Query("select * from users").in("created_at", values),
      "select * from users where created_at in ({created_at}::timestamptz, {created_at2}::timestamptz)",
      "select * from users where created_at in " + values.mkString("('", "'::timestamptz, '", "'::timestamptz)")
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
      "select * from users where age = {age}::int",
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

  it("in with uuid") {
    val guids = Seq(UUID.randomUUID, UUID.randomUUID)
    validate(
      Query("select * from users").in("guid", guids),
      "select * from users where guid in ({guid}::uuid, {guid2}::uuid)",
      "select * from users where guid in " + guids.mkString("('", "'::uuid, '", "'::uuid)")
    )
  }
  

  it("datetime") {
    val ts = DateTime.now
    validate(
      Query("select * from users").equals("users.created_at", None),
      "select * from users",
      "select * from users"
    )

    validate(
      Query("select * from users").equals("users.created_at", Some(ts)),
      "select * from users where users.created_at = {created_at}::timestamptz",
      s"select * from users where users.created_at = '$ts'::timestamptz"
    )
  }
  
  it("unit") {
    validate(
      Query("insert into users (first) values ({first})").bind("first", None),
      "insert into users (first) values ({first})",
      "insert into users (first) values (null)"
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

  it("bind int") {
    validate(
      Query("select * from users").
        and("age = {age}::int").
        bind("age", 5),
      "select * from users where age = {age}::int",
      "select * from users where age = 5"
    )

    validate(
      Query("select * from users").
        and("age = {age}").
        bind("age", 5),
      "select * from users where age = {age}",
      "select * from users where age = 5"
    )
  }

  it("bind long") {
    validate(
      Query("select * from users").
        and("age = {age}::bigint").
        bind("age", 5l),
      "select * from users where age = {age}::bigint",
      "select * from users where age = 5"
    )

    validate(
      Query("select * from users").
        and("age = {age}").
        bind("age", 5l),
      "select * from users where age = {age}",
      "select * from users where age = 5"
    )
  }

  it("bind numeric") {
    validate(
      Query("select * from users").
        and("age = {age}::numeric").
        bind("age", 5.5),
      "select * from users where age = {age}::numeric",
      "select * from users where age = 5.5"
    )

    validate(
      Query("select * from users").
        and("age = {age}").
        bind("age", 5.5),
      "select * from users where age = {age}",
      "select * from users where age = 5.5"
    )
  }

  it("bind uuid") {
    val guid = UUID.randomUUID

    validate(
      Query("select * from users where guid = {guid}::uuid").
      bind("guid", guid),
      "select * from users where guid = {guid}::uuid",
      s"select * from users where guid = '$guid'::uuid"
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
        ex.getMessage should be("assertion failed: Bind variable named 'email' already defined")
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

  it("isTrue") {
    validate(
      Query("select * from users").isTrue("users.verified"),
      "select * from users where users.verified is true",
      "select * from users where users.verified is true"
    )
  }

  it("isFalse") {
    validate(
      Query("select * from users").isFalse("users.verified"),
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

  it("debuggingInfo simple query") {
    Query("select * from users").debuggingInfo() should be("select * from users")
  }

  it("debuggingInfo query w/ bind vars") {
    val q = Query("select * from users").equals("id", Some(5))
    q.debuggingInfo() should be(
      Seq(
        "select * from users where id = {id}::int",
        " - id: 5",
        "Interpolated:",
        "select * from users where id = 5"
      ).mkString("\n")
    )
  }

  it("queries that share bind variables (note status2 in bind key)") {
    val experience = Query("select * from experiences")
    val liveFilter = Query("select id from experiences").equals("status", "draft")
    val draftFilter = Query("select id from experiences").equals("status", "live")



    validate(
      experience.copy(
        bind = experience.bind ++ liveFilter.bind ++ draftFilter.bind
      ).or(
        Seq(
          "experience.id in (${liveFilter.sql()})",
          "experience.id in (${draftFilter.sql()})"
        )
      ),
      "select * from users where status = trim({status}) and id in (select id from users where status = trim({status2}))",
      "select * from users where status = trim('live') and id in (select id from users where status = trim('draft'))"
    )
  }

}

package io.flow.postgresql

import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success, Try}

class CombinedQuerySpec extends AnyFunSpec with Matchers {

  def validate(combinedQuery: SQLBase, sql: String, interpolate: String): Assertion = {
    combinedQuery.sql() should be(sql.trim)
    combinedQuery.interpolate() should be(interpolate.trim)
  }

  def createUnion() =
    Query("select id from users where id = '12345'") union Query("select id from users where id = '54321'")

  it("union") {
    val query1 = Query("select id from users").equals("id", "12345")
    val query2 = Query("select id from users").equals("id", "54321")
    validate(
      combinedQuery = query1 union query2,
      """
        |(select id from users where id = trim({id}))
        |union
        |(select id from users where id = trim({id}))
        |""".stripMargin,
      """
        |(select id from users where id = trim('12345'))
        |union
        |(select id from users where id = trim('54321'))
        |""".stripMargin
    )
  }

  it("intersect") {
    val query1 = Query("select id from users").in("id", Seq("12345", "54321"))
    val query2 = Query("select id from users").equals("id", "54321")
    validate(
      combinedQuery = query1 intersect query2,
      """
        |(select id from users where id in (trim({id}), trim({id2})))
        |intersect
        |(select id from users where id = trim({id}))
        |""".stripMargin,
      """
        |(select id from users where id in (trim('12345'), trim('54321')))
        |intersect
        |(select id from users where id = trim('54321'))
        |""".stripMargin
    )
  }

  it("except") {
    val query1 = Query("select id from users").in("id", Seq("12345", "54321"))
    val query2 = Query("select id from users").equals("id", "54321")
    validate(
      combinedQuery = query1 except query2,
      """
        |(select id from users where id in (trim({id}), trim({id2})))
        |except
        |(select id from users where id = trim({id}))
        |""".stripMargin,
      """
        |(select id from users where id in (trim('12345'), trim('54321')))
        |except
        |(select id from users where id = trim('54321'))
        |""".stripMargin
    )
  }

  it("debuggingInfo query w/ bind vars") {
    val query1 = Query("select * from users").equals("id", Some(5))
    val query2 = Query("select * from users").equals("id", Some(6))
    (query1 union query2).debuggingInfo() should be(
      Seq(
        "(select * from users where id = {id}::int",
        " - id: 5",
        "Interpolated:",
        "select * from users where id = 5)",
        "union",
        "(select * from users where id = {id}::int",
        " - id: 6",
        "Interpolated:",
        "select * from users where id = 6)"
      ).mkString("\n")
    )
  }

  it("binding on query") {
    val query1 = Query("select id from users").and("email = {email}").bind("email", Some("foo@flow.io"))
    val query2 = Query("select id from users").and("email = {email}").bind("email", Some("bar@flow.io"))
    validate(
      combinedQuery = query1 union query2,
      """
        |(select id from users where email = {email})
        |union
        |(select id from users where email = {email})
        |""".stripMargin,
      """
        |(select id from users where email = 'foo@flow.io')
        |union
        |(select id from users where email = 'bar@flow.io')
        |""".stripMargin
    )
    (query1 union query2).anormSql()
    ()
  }

  it("bind on a combined query") {
    val query1 = Query("select id from users").and("email = {email}")
    val query2 = Query("select id from users").and("email = {email}")
    validate(
      combinedQuery = (query1 union query2).bind("email", "foo@flow.io"),
      """
        |(select id from users where email = {email})
        |union
        |(select id from users where email = {email})
        |""".stripMargin,
      """
        |(select id from users where email = 'foo@flow.io')
        |union
        |(select id from users where email = 'foo@flow.io')
        |""".stripMargin
    )
  }

  it("bind on a combined query with multiple variables") {
    val query1 = Query("select id from users").and("email = {email}").bind("email", Some("foo@flow.io"))
    val query2 = Query("select id from users").and("id = {id}")
    validate(
      combinedQuery = (query1 intersect query2).bind("id", "123456"),
      """
        |(select id from users where email = {email})
        |intersect
        |(select id from users where id = {id})
        |""".stripMargin,
      """
        |(select id from users where email = 'foo@flow.io')
        |intersect
        |(select id from users where id = '123456')
        |""".stripMargin
    )
  }

  it("bind on a mix of query and combined query") {
    val query1 = Query("select id from users").and("email = {email}")
    val query2 = Query("select id from users").and("id = {id}")
    validate(
      combinedQuery = (query1 intersect query2)
        .bind("id", "123456")
        .bind("email", "foo@flow.io"),
      """
        |(select id from users where email = {email})
        |intersect
        |(select id from users where id = {id})
        |""".stripMargin,
      """
        |(select id from users where email = 'foo@flow.io')
        |intersect
        |(select id from users where id = '123456')
        |""".stripMargin
    )
  }

  it("bind with duplicate variable name raises an error") {
    val query1 = Query("select id from users").and("email = {email}").bind("email", "foo@flow.io")
    val query2 = Query("select id from users").and("email = {email}").bind("email", "bar@flow.io")
    Try {
      (query1 union query2).bind("email", "foo@flow.io")
    } match {
      case Success(_) => fail("Expected error for duplicate bind variable")
      case Failure(ex) => {
        ex.getMessage should be("assertion failed: Bind variable named 'email' already defined")
      }
    }
  }
}

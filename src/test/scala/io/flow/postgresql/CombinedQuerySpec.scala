package io.flow.postgresql

import anorm.SqlParser
import io.flow.postgresql.db.TestDatabase
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class CombinedQuerySpec extends AnyFunSpec with Matchers {

  def validate(combinedQuery: SQLBase, sql: String, interpolate: String): Assertion = {
    combinedQuery.sql() should be(sql.trim)
    combinedQuery.interpolate() should be(interpolate.trim)
  }

  it("union") {
    val query1 = Query("select id from test_users").equals("id", "12345")
    val query2 = Query("select id from test_users").equals("id", "54321")
    val union = query1 union query2
    validate(
      combinedQuery = union,
      """
        |(select id from test_users where id = trim({id}))
        |union
        |(select id from test_users where id = trim({id}))
        |""".stripMargin,
      """
        |(select id from test_users where id = trim('12345'))
        |union
        |(select id from test_users where id = trim('54321'))
        |""".stripMargin
    )

    TestDatabase.withConnection { implicit c =>
      union.as(SqlParser.str("id").*)
    }
  }

  it("intersect") {
    val query1 = Query("select id from test_users").in("id", Seq("12345", "54321"))
    val query2 = Query("select id from test_users").equals("id", "54321")
    validate(
      combinedQuery = query1 intersect query2,
      """
        |(select id from test_users where id in (trim({id}), trim({id2})))
        |intersect
        |(select id from test_users where id = trim({id}))
        |""".stripMargin,
      """
        |(select id from test_users where id in (trim('12345'), trim('54321')))
        |intersect
        |(select id from test_users where id = trim('54321'))
        |""".stripMargin
    )
  }

  it("except") {
    val query1 = Query("select id from test_users").in("id", Seq("12345", "54321"))
    val query2 = Query("select id from test_users").equals("id", "54321")
    validate(
      combinedQuery = query1 except query2,
      """
        |(select id from test_users where id in (trim({id}), trim({id2})))
        |except
        |(select id from test_users where id = trim({id}))
        |""".stripMargin,
      """
        |(select id from test_users where id in (trim('12345'), trim('54321')))
        |except
        |(select id from test_users where id = trim('54321'))
        |""".stripMargin
    )
  }

  it("debuggingInfo query w/ bind vars") {
    val query1 = Query("select * from test_users").equals("id", Some(5))
    val query2 = Query("select * from test_users").equals("id", Some(6))
    (query1 union query2).debuggingInfo should be(
      Seq(
        "(select * from test_users where id = {id}::int",
        " - id: 5",
        "Interpolated:",
        "select * from test_users where id = 5)",
        "union",
        "(select * from test_users where id = {id}::int",
        " - id: 6",
        "Interpolated:",
        "select * from test_users where id = 6)"
      ).mkString("\n")
    )
  }

  it("binding on query") {
    val query1 = Query("select id from test_users").and("name = {name}").bind("name", "foo")
    val query2 = Query("select id from test_users").and("name = {name}").bind("name", "bar")
//    validate(
//      combinedQuery = query1 union query2,
//      """
//        |(select id from test_users where email = {email})
//        |union
//        |(select id from test_users where email = {email})
//        |""".stripMargin,
//      """
//        |(select id from test_users where email = 'foo@flow.io')
//        |union
//        |(select id from test_users where email = 'bar@flow.io')
//        |""".stripMargin
//    )
    db.TestDatabase.withConnection { implicit c =>
      println("1")
      println(query1.allBindVariables)
      println(query1.as(SqlParser.str("id").*))

      println("2")
      println(query2.allBindVariables)
      println(query2.as(SqlParser.str("id").*))

      println("union")
      val union = (query1 except query2)
      val res = union.as(SqlParser.str("id").*)
      println(res)
    }
    ()
  }

  it("bind on a combined query") {
    val query1 = Query("select id from test_users").and("email = {email}")
    val query2 = Query("select id from test_users").and("email = {email}")
    validate(
      combinedQuery = (query1 union query2).bind("email", "foo@flow.io"),
      """
        |(select id from test_users where email = {email})
        |union
        |(select id from test_users where email = {email})
        |""".stripMargin,
      """
        |(select id from test_users where email = 'foo@flow.io')
        |union
        |(select id from test_users where email = 'foo@flow.io')
        |""".stripMargin
    )
  }

  it("bind on a combined query with multiple variables") {
    val query1 = Query("select id from test_users").and("email = {email}").bind("email", Some("foo@flow.io"))
    val query2 = Query("select id from test_users").and("id = {id}")
    validate(
      combinedQuery = (query1 intersect query2).bind("id", "123456"),
      """
        |(select id from test_users where email = {email})
        |intersect
        |(select id from test_users where id = {id})
        |""".stripMargin,
      """
        |(select id from test_users where email = 'foo@flow.io')
        |intersect
        |(select id from test_users where id = '123456')
        |""".stripMargin
    )
  }

  it("bind on a mix of query and combined query") {
    val query1 = Query("select id from test_users").and("email = {email}")
    val query2 = Query("select id from test_users").and("id = {id}")
    validate(
      combinedQuery = (query1 intersect query2)
        .bind("id", "123456")
        .bind("email", "foo@flow.io"),
      """
        |(select id from test_users where email = {email})
        |intersect
        |(select id from test_users where id = {id})
        |""".stripMargin,
      """
        |(select id from test_users where email = 'foo@flow.io')
        |intersect
        |(select id from test_users where id = '123456')
        |""".stripMargin
    )
  }

  it("bind with duplicate variable name raises an error") {
    val query1 = Query("select id from test_users").and("email = {email}").bind("email", "foo@flow.io")
    val query2 = Query("select id from test_users").and("email = {email}").bind("email", "bar@flow.io")

    val ex = the [Throwable] thrownBy (query1 union query2)
    ex.getMessage should include ("duplicate bound variables")
  }

  it("ben") {
    println {
//      val query1 = Query("select id from test_users").equals("id", "12345").equals("id", "456789")
//      val query2 = Query("select id from test_users").equals("id", "54321")
//      (query1 union query2).interpolate

      // should pass
      val query1 = Query("select id from test_users").and("id = {id}")
      val query2 = Query("select id from test_users").and("id = {id}")
      (query1 union query2).bind("id", "12335").sql()

      // should fail
      val query3 = Query("select id from test_users").and("id = {id}").bind("id", "12345")
      val query4 = Query("select id from test_users").and("id = {id}").bind("id", "54321")
      (query3 union query4).sql()

      //      Query("select 1")
//        .equals("id", 3)
//        .equals("id", 4)
//        .equals("id", 5)
//        .debuggingInfo
    }
  }

}

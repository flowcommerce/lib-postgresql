package io.flow.postgresql

import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class CombinedQuerySpec extends AnyFunSpec with Matchers {

  def validate(combinedQuery: CombinedQuery, sql: String, interpolate: String): Assertion = {
    combinedQuery.sql() should be(sql.trim)
    combinedQuery.interpolate() should be(interpolate.trim)
  }

  def createUnion() =
    Query("select id from users where id = '12345'") union Query("select id from users where id = '54321'")

  it("union") {
    validate(
      combinedQuery = Query("select id from users where id = '12345'") union Query("select id from users where id = '54321'"),
      """
        |(select id from users where id = '12345')
        |union
        |(select id from users where id = '54321')
        |""".stripMargin,
      """
        |(select id from users where id = '12345')
        |union
        |(select id from users where id = '54321')
        |""".stripMargin
    )
  }

  it("debuggingInfo simple query") {
    createUnion().debuggingInfo() should be(
      """
        |(select id from users where id = '12345')
        |union
        |(select id from users where id = '54321')
        |""".stripMargin.stripLeading.stripTrailing
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

}

package io.flow.postgresql

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class OrderBySpec extends AnyFunSpec with Matchers {
  def rightOrErrors[T](value: Either[_, T]): T = {
    value match {
      case Left(_) => sys.error(s"Expected a right value but got: $value")
      case Right(r) => r
    }
  }

  def leftOrErrors[T](value: Either[T, _]): T = {
    value match {
      case Left(r) => r
      case Right(_) => sys.error(s"Expected a left value but got: $value")
    }
  }

  it("removeOrder") {
    OrderBy.removeOrder("name") should be("name")
    OrderBy.removeOrder("-name") should be("name")
    OrderBy.removeOrder("+name") should be("name")
  }

  it("removeFunctions") {
    OrderBy.removeFunctions("lower(name)") should be("name")
    OrderBy.removeFunctions("json(name)") should be("name")
    OrderBy.removeFunctions("abs(name)") should be("name")
    OrderBy.removeFunctions("other(name)") should be("other(name)")
  }

  it("empty") {
    OrderBy("").sql should be(None)
    OrderBy("    ").sql should be(None)
    OrderBy("  ,  ").sql should be(None)
  }

  it("case") {
    OrderBy("key").sql.get should be("key")
    OrderBy("KEY").sql.get should be("KEY")
    OrderBy("kEy").sql.get should be("kEy")
  }

  it("direction") {
    OrderBy("key").sql.get should be("key")
    OrderBy("-key").sql.get should be("key desc")
    OrderBy("+key").sql.get should be("key")
  }

  it("composite") {
    OrderBy("key1,key2").sql.get should be("key1, key2")
    OrderBy("key1, key2").sql.get should be("key1, key2")
    OrderBy("-key1,-key2").sql.get should be("key1 desc, key2 desc")
    OrderBy(" +key1 , +key2 ").sql.get should be("key1, key2")
    OrderBy("-lower(projects.key),created_at", Some("organizations")).sql.get should be(
      "lower(projects.key) desc, organizations.created_at"
    )
    OrderBy("-abs(projects.value),created_at", Some("organizations")).sql.get should be(
      "abs(projects.value) desc, organizations.created_at"
    )
  }

  it("defaultTable") {
    OrderBy("key", Some("organizations")).sql.get should be("organizations.key")
    OrderBy("-key", Some("organizations")).sql.get should be("organizations.key desc")
    OrderBy("+key", Some("organizations")).sql.get should be("organizations.key")
  }

  it("defaultTable when not needed") {
    OrderBy("projects.key", Some("organizations")).sql.get should be("projects.key")
    OrderBy("-projects.key", Some("organizations")).sql.get should be("projects.key desc")
    OrderBy("+projects.key", Some("organizations")).sql.get should be("projects.key")
  }

  it("function with direction") {
    OrderBy("lower(key)").sql.get should be("lower(key)")
    OrderBy("-lower(key)").sql.get should be("lower(key) desc")
    OrderBy("+lower(key)").sql.get should be("lower(key)")

    OrderBy("abs(key)").sql.get should be("abs(key)")
    OrderBy("-abs(key)").sql.get should be("abs(key) desc")
    OrderBy("+abs(key)").sql.get should be("abs(key)")
  }

  it("function with defaultTable") {
    OrderBy("lower(key)", Some("organizations")).sql.get should be("lower(organizations.key)")
    OrderBy("-lower(key)", Some("organizations")).sql.get should be("lower(organizations.key) desc")
    OrderBy("+lower(key)", Some("organizations")).sql.get should be("lower(organizations.key)")
  }

  it("function with defaultTable when not needed") {
    OrderBy("lower(projects.key)", Some("organizations")).sql.get should be("lower(projects.key)")
    OrderBy("-lower(projects.key)", Some("organizations")).sql.get should be("lower(projects.key) desc")
    OrderBy("+lower(projects.key)", Some("organizations")).sql.get should be("lower(projects.key)")
  }

  it("function with json field") {
    OrderBy("json(data.name)", None).sql.get should be("data->>'name'")
    OrderBy("-json(data.name)", None).sql.get should be("data->>'name' desc")
    OrderBy("json(data.price.amount)", None).sql.get should be("(data->>'price')::jsonb->'amount'")
    OrderBy("-json(data.price.amount)", None).sql.get should be("(data->>'price')::jsonb->'amount' desc")
  }

  it("rejects invalid json function") {
    OrderBy.parse("json(foo)", None) should be(
      Left(Seq("Error defining json query column[foo]: Must be column.field[.field]"))
    )
  }

  it("respects validValues") {
    def expectValid(value: String) = {
      rightOrErrors {
        OrderBy.parse(value, validValues = Some(Set("id", "name")))
      }.sql
    }
    expectValid("id") should equal(Some("id"))
    expectValid("+id") should equal(Some("id"))
    expectValid("-id") should equal(Some("id desc"))
    expectValid("lower(id)") should equal(Some("lower(id)"))
    expectValid("name") should equal(Some("name"))
    expectValid("-name") should equal(Some("name desc"))
    expectValid("lower(id), -name") should equal(Some("lower(id), name desc"))

    expectValid("ID") should equal(Some("ID"))
    expectValid("NAME") should equal(Some("NAME"))
  }

  it("reports errors if clause contains a value not specified in validValues") {
    def expectInvalid(value: String) = {
      leftOrErrors {
        OrderBy.parse(value, validValues = Some(Set("id", "name")))
      } should equal(
        Seq("The following value is invalid: foo. Must be one of: id, name")
      )
    }
    expectInvalid("foo")
    expectInvalid("-foo")
    expectInvalid("lower(foo)")
  }

  it("only allows parentheses around known functions") {
    OrderBy.parse("other_lower(foo)", None) should be(
      Left(Seq("Sort[other_lower(foo)] contains invalid characters: '(', ')'"))
    )
  }

  it("validates function names") {
    OrderBy.parse("case(user)") should be(
      Left(
        Seq(
          "Sort[case(user)] contains invalid characters: '(', ')'"
        )
      )
    )
  }

  it("prevents sql injection") {
    OrderBy.parse("drop table users") should be(
      Left(Seq("Sort[drop table users] contains invalid characters: ' '"))
    )

    OrderBy.parse("name,name||pg_sleep(5)--") should be(
      Left(Seq("Sort[name,name||pg_sleep(5)--] contains invalid characters: '|', '(', ')'"))
    )
  }

  it("is strict on characters allowed") {
    OrderBy.parse("*") should be(
      Left(Seq("Sort[*] contains invalid characters: '*'"))
    )
    OrderBy.parse(";") should be(
      Left(Seq("Sort[;] contains invalid characters: ';'"))
    )
  }

  it("rejects invalid function") {
    OrderBy.parse("foo(key)") should be(Left(Seq("Sort[foo(key)] contains invalid characters: '(', ')'")))
    OrderBy.parse("key,foo(key)") should be(Left(Seq("Sort[key,foo(key)] contains invalid characters: '(', ')'")))
    OrderBy.parse("key,foo(key),bar(key)") should be(
      Left(
        Seq(
          "Sort[key,foo(key),bar(key)] contains invalid characters: '(', ')'"
        )
      )
    )
  }

  it("append") {
    val a = OrderBy("-lower(projects.key)")
    val combined = a.append("-created_at")
    combined.sql should be(Some("lower(projects.key) desc, -created_at"))
  }

}

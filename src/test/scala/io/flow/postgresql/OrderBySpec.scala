package io.flow.postgresql

import org.scalatest.{FunSpec, Matchers}

class OrderBySpec extends FunSpec with Matchers {

  it("empty") {
    OrderBy("").sql should be(None)
    OrderBy("    ").sql should be(None)
    OrderBy("  ,  ").sql should be(None)
  }

  it("direction") {
    OrderBy("key").sql.get should be("key")
    OrderBy("-key").sql.get should be("key desc")
    OrderBy("+key").sql.get should be("key")
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
    OrderBy.parse("json(foo)", None) should be(Left(Seq("Error defining json query column[foo]: Must be column.field[.field]")))
  }

  it("composite") {
    OrderBy("-lower(projects.key),created_at", Some("organizations")).sql.get should be(
      "lower(projects.key) desc, organizations.created_at"
    )
  }

  it("prevents sql injection") {
    OrderBy.parse("drop table users") should be(
      Left(Seq("Sort[drop table users] contained a space which is not allowed"))
    )
  }

  it("rejects invalid function") {
    OrderBy.parse("foo(key)") should be(Left(Seq("foo(key): Invalid function[foo]. Must be one of: lower, json")))
    OrderBy.parse("key,foo(key)") should be(Left(Seq("foo(key): Invalid function[foo]. Must be one of: lower, json")))
    OrderBy.parse("key,foo(key),bar(key)") should be(Left(
      Seq(
        "foo(key): Invalid function[foo]. Must be one of: lower, json",
        "bar(key): Invalid function[bar]. Must be one of: lower, json"
      )
    ))
  }

  it("append") {
    val a = OrderBy("-lower(projects.key)")
    val combined = a.append("-created_at")
    combined.sql should be(Some("lower(projects.key) desc, -created_at"))
  }

}

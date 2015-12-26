package io.flow.play.postgresql

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

}

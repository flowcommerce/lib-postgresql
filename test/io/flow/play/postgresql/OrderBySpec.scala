package io.flow.play.postgresql

import org.scalatest.{FunSpec, Matchers}

class OrderBySpec extends FunSpec with Matchers {

  it("empty") {
    OrderBy.parseOrError("").sql should be(None)
    OrderBy.parseOrError("    ").sql should be(None)
    OrderBy.parseOrError("  ,  ").sql should be(None)
  }

  it("direction") {
    OrderBy.parseOrError("key").sql.get should be("key")
    OrderBy.parseOrError("-key").sql.get should be("key desc")
    OrderBy.parseOrError("+key").sql.get should be("key")
  }

  it("defaultTable") {
    OrderBy.parseOrError("key", Some("organizations")).sql.get should be("organizations.key")
    OrderBy.parseOrError("-key", Some("organizations")).sql.get should be("organizations.key desc")
    OrderBy.parseOrError("+key", Some("organizations")).sql.get should be("organizations.key")
  }

  it("defaultTable when not needed") {
    OrderBy.parseOrError("projects.key", Some("organizations")).sql.get should be("projects.key")
    OrderBy.parseOrError("-projects.key", Some("organizations")).sql.get should be("projects.key desc")
    OrderBy.parseOrError("+projects.key", Some("organizations")).sql.get should be("projects.key")
  }

  it("function with direction") {
    OrderBy.parseOrError("lower(key)").sql.get should be("lower(key)")
    OrderBy.parseOrError("-lower(key)").sql.get should be("lower(key) desc")
    OrderBy.parseOrError("+lower(key)").sql.get should be("lower(key)")
  }

  it("function with defaultTable") {
    OrderBy.parseOrError("lower(key)", Some("organizations")).sql.get should be("lower(organizations.key)")
    OrderBy.parseOrError("-lower(key)", Some("organizations")).sql.get should be("lower(organizations.key) desc")
    OrderBy.parseOrError("+lower(key)", Some("organizations")).sql.get should be("lower(organizations.key)")
  }

  it("function with defaultTable when not needed") {
    OrderBy.parseOrError("lower(projects.key)", Some("organizations")).sql.get should be("lower(projects.key)")
    OrderBy.parseOrError("-lower(projects.key)", Some("organizations")).sql.get should be("lower(projects.key) desc")
    OrderBy.parseOrError("+lower(projects.key)", Some("organizations")).sql.get should be("lower(projects.key)")
  }

  it("composite") {
    OrderBy.parseOrError("-lower(projects.key),created_at", Some("organizations")).sql.get should be(
      "lower(projects.key) desc, organizations.created_at"
    )
  }

}

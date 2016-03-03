package io.flow.postgresql

import org.scalatest.{FunSpec, Matchers}

class BindVariableSpec extends FunSpec with Matchers {

  it("safeName") {
    BindVariable.safeName("user") should be("user")
    BindVariable.safeName("users.first") should be("first")
    BindVariable.safeName("users first") should be("users_first")
    BindVariable.safeName("users.first_") should be("first")

    BindVariable.safeName("_first") should be("first")
    BindVariable.safeName("_users.first") should be("first")

    BindVariable.safeName("users.first_name") should be("first_name")
    BindVariable.safeName("users.first___name") should be("first_name")
    BindVariable.safeName("users.json->>'id'") should be("json_id")
  }

  it("safeName is never empty") {
    BindVariable.safeName("") should be("bind")
    BindVariable.safeName("   ") should be("bind")
    BindVariable.safeName("____") should be("bind")
  }
  
}

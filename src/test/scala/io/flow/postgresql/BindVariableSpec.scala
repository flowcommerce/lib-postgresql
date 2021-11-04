package io.flow.postgresql

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BindVariableSpec extends AnyFunSpec with Matchers {

  it("safeName") {
    BindVariable.safeName("user") should be("user")
    BindVariable.safeName(" user ") should be("user")
    BindVariable.safeName("_first") should be("first")
    BindVariable.safeName("first_") should be("first")
    BindVariable.safeName("_first_") should be("first")
    BindVariable.safeName("___first___") should be("first")
    BindVariable.safeName("users.first") should be("first")
    BindVariable.safeName("users first") should be("users_first")
    BindVariable.safeName("users.first_") should be("first")
    BindVariable.safeName("users._first") should be("first")
    BindVariable.safeName("users._first_") should be("first")
    BindVariable.safeName("_users.first") should be("first")
    BindVariable.safeName(" users. first ") should be("first")

    BindVariable.safeName("users.first_name") should be("first_name")
    BindVariable.safeName("users.first__name") should be("first_name")
    BindVariable.safeName("users.first___name") should be("first_name")
    BindVariable.safeName("users.json->>'id'") should be("json_id")
    BindVariable.safeName("UsEr") should be("user")
    BindVariable.safeName("UsErS.FiRsT") should be("first")
    BindVariable.safeName("üšéRŸ") should be("r")
    BindVariable.safeName("üUšSéEęRŸ") should be("u_s_e_r")

    BindVariable.safeName("0123456789") should be("0123456789")
    BindVariable.safeName("abcdefghijklmnopqrstuvwxyz") should be("abcdefghijklmnopqrstuvwxyz")
    BindVariable.safeName("ABCDEFGHIJKLMNOPQRSTUVWXYZ") should be("abcdefghijklmnopqrstuvwxyz")
  }

  it("safeName is never empty") {
    BindVariable.safeName("") should be("bind")
    BindVariable.safeName(":") should be("bind")
    BindVariable.safeName("_") should be("bind")
    BindVariable.safeName("   ") should be("bind")
    BindVariable.safeName("____") should be("bind")
    BindVariable.safeName("üšéŸß") should be("bind")
  }
  
}

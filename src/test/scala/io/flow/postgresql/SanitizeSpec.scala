package io.flow.postgresql

import org.scalatest.{FunSpec, Matchers}

class SanitizeSpec extends FunSpec with Matchers {

  it("prevents sql injection") {
    Sanitize.isSafe("foo") should be(true)
    Sanitize.isSafe("drop table users") should be(false)
    Sanitize.isSafe("name,name||pg_sleep(5)--") should be(false)
  }

}

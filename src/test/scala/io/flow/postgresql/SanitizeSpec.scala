package io.flow.postgresql

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SanitizeSpec extends AnyFunSpec with Matchers {

  it("prevents sql injection") {
    Sanitize.isSafe("foo") should be(true)
    Sanitize.isSafe("drop table users") should be(false)
    Sanitize.isSafe("name,name||pg_sleep(5)--") should be(false)
  }

}

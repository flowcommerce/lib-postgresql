package io.flow.postgresql

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class DebugOrQuerySpec extends AnyFunSpec with Matchers {

  it("debugOrClausesToWrap") {
    val q = Query("select count(*) from test_users").or("id = 1 of id=2")
    q.orClausesToWrap.size shouldBe 1
    val el = q.orClausesToWrap.head
    el._1 shouldBe "id = 1 of id=2"
    el._2 shouldBe "(id = 1 of id=2)"
  }

}

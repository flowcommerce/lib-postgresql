package io.flow.postgresql

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class DebugOrQuerySpec extends AnyFunSpec with Matchers {

  it("debugOrClausesToWrap") {
    val q = Query("select count(*) from test_users").or("id = 1 or id=2")
    q.orClausesToWrap.size shouldBe 1
    val el = q.orClausesToWrap.head
    el._1 shouldBe "id = 1 or id=2"
    el._2 shouldBe "(id = 1 or id=2)"
  }

  it("debugOrClausesToWrap prints once per query") {
    val q1 = Query("select count(*) from test_users").or("id = 1 or id=2")
    val q2 = Query("select count(*) from test_users").or("id = 1 or id=3")
    0.to(100).foreach { _ =>
      q1.anormSql()
      q2.anormSql()
    }
  }

}

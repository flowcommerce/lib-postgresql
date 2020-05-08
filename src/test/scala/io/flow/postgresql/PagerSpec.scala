package io.flow.postgresql

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class PagerSpec extends AnyFunSpec with Matchers {

  it("empty list") {
    val pager = Pager.create { _ => Nil }
    pager.hasNext should be(false)

    intercept[NoSuchElementException] {
      pager.next()
    }
  }

  it("single page list") {
    val pager = Pager.create { offset =>
      offset match {
        case 0 => Seq(1, 2)
        case _ => Nil
      }
    }

    pager.hasNext should be(true)
    pager.next should be(1)
    pager.hasNext should be(true)
    pager.next should be(2)
    pager.hasNext should be(false)
  }

  it("multiple pages") {
    val pager = Pager.create { offset =>
      offset match {
        case 0 => Seq("a", "b")
        case 2 => Seq("c")
        case _ => Nil
      }
    }

    pager.next should be("a")
    pager.next should be("b")
    pager.next should be("c")
    pager.hasNext should be(false)
  }

}

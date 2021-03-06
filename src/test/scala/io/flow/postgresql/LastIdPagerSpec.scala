package io.flow.postgresql

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class LastIdPagerSpec extends AnyFunSpec with Matchers {

  it("empty list") {
    val pager = Pager.byLastId[Long, Long](_ => Nil, identity)
    pager.hasNext should be(false)

    intercept[NoSuchElementException] {
      pager.next()
    }
  }

  it("single page list") {
    val pager = Pager.byLastId[String, Int](
      {
        case None => Seq("*", "**")
        case Some(2L) => Nil
        case other => sys.error(s"unexpected value: $other")
      },
      _.size
    )

    pager.hasNext should be(true)
    pager.next() should be("*")
    pager.hasNext should be(true)
    pager.next() should be("**")
    pager.hasNext should be(false)
  }

  it("multiple pages") {
    val pager = Pager.byLastId[String, Int](
      {
        case None => Seq("*", "**")
        case Some(2L) => Seq("****")
        case Some(4L) => Nil
        case other => sys.error(s"unexpected value: $other")
      },
      _.size
    )

    pager.hasNext should be(true)
    pager.next() should be("*")
    pager.hasNext should be(true)
    pager.next() should be("**")
    pager.hasNext should be(true)
    pager.next() should be("****")
    pager.hasNext should be(false)
  }

}

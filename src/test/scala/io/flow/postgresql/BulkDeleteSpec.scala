package io.flow.postgresql

import org.scalatest.{FunSpec, Matchers}

class BulkDeleteSpec extends FunSpec with Matchers {

  it("empty list") {
    val emptyList = Seq()

    BulkDelete.byPage {
      emptyList
    } { e =>
      sys.error("Should not have iterator")
    }
  }

  it("list with single element") {
    val original = Seq("x")
    var items = original
    var found = scala.collection.mutable.ListBuffer[String]()

    BulkDelete.byPage {
      items
    } { e =>
      found += e
      items = items.filter(_ != e)
    }
    found should equal(original)
  }

  it("list with multiple elements") {
    val original = Seq(1, 2, 3)
    var items = original
    var found = scala.collection.mutable.ListBuffer[Int]()

    BulkDelete.byPage {
      println("ADSFASDF")
      items
    } { e =>
      found += e
      items = items.filter(_ != e)
    }
    found should equal(original)
  }

}

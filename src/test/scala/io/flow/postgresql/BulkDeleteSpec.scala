package io.flow.postgresql

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BulkDeleteSpec extends AnyFunSpec with Matchers {

  it("empty list") {
    val emptyList: List[String] = Nil

    BulkDelete.byPage {
      emptyList
    } { _ =>
      sys.error("Should not have iterator")
    }
  }

  it("list with single element") {
    val original = Seq("x")
    var items = original
    val found = scala.collection.mutable.ListBuffer[String]()

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
    val found = scala.collection.mutable.ListBuffer[Int]()

    BulkDelete.byPage {
      items
    } { e =>
      found += e
      items = items.filter(_ != e)
    }
    found should equal(original)
  }

}

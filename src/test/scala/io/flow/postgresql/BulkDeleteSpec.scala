package io.flow.postgresql

import org.scalatest.{FunSpec, Matchers}

class BulkDeleteSpec extends FunSpec with Matchers {

  def deleteFunction(item: String): String = {
    println(s"Element [$item] deleted")
    item
  }

  it("empty list") {
    val emptyList = Seq()

    BulkDelete.byPage {
      emptyList
    } { e =>
      deleteFunction(e)
    }
  }

  it("list with single element") {
    var listWithOneElement = Seq("x")
    BulkDelete.byPage {
      listWithOneElement
    } { e =>
      val d = deleteFunction(e)

      if(d == "x")
        listWithOneElement = Seq()
    }
  }

  it("list with multiple elements") {
    var listWithMultiElement = Seq("a", "b", "c")
    BulkDelete.byPage {
      listWithMultiElement
    } { e =>
      val d = deleteFunction(e)

      if(d == "c")
        listWithMultiElement = Seq()
    }
  }

}

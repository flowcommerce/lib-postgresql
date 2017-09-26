package io.flow.postgresql

import org.scalatest.{FunSpec, Matchers}


class LockingSpec extends FunSpec with Matchers {

  it("locking clause") {
    val query = Query("select * from table").equals("col", "val").limit(10).locking("for update skip locked")
    query.sql() should endWith("for update skip locked")
  }

}

package io.flow.postgresql

trait Clause {

  def sql: String

}

object Clause {

  case object True extends Clause {
    override val sql: String = "true"
  }

  case object False extends Clause {
    override val sql: String = "false"
  }

  case class Or(conditions: Seq[String]) extends Clause {
    assert(!conditions.isEmpty, "Must have at least one condition")

    override val sql: String = conditions match {
      case Nil => "false"
      case one :: Nil => one
      case multiple => "(" + multiple.mkString(" or ") + ")"
    }

  }

  def single(condition: String): Or = {
    Or(Seq(condition))
  }

}

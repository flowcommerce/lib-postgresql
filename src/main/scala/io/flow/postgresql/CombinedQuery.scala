package io.flow.postgresql

import anorm.NamedParameter

sealed trait CombinedQuery extends SQLBase {
  protected def left: SQLBase
  protected def right: SQLBase
  protected def operation: String

  override def namedParameters(): Seq[NamedParameter] =
    left.namedParameters() ++ right.namedParameters()

  override def sql(): String = s"(${left.sql()})\n$operation\n(${right.sql()})"

  override def debuggingInfo(): String = s"(${left.debuggingInfo()})\n$operation\n(${right.debuggingInfo()})"

  override def interpolate(): String = s"(${left.interpolate()})\n$operation\n(${right.interpolate()})"

  def bind[T](
    name: String,
    value: Option[T]
  ): SQLBase = {
    value match {
      case None => bind(name, ())
      case Some(v) => bind(name, v)
    }
  }
}

case class Union(
  left: SQLBase,
  right: SQLBase,
  debug: Boolean = false,
) extends CombinedQuery {

  override val operation = "union"

  override def bind[T](name: String, value: T): SQLBase = {
    this.copy(
      left = left.bind(name, value),
      right = right.bind(name, value)
    )
  }

  override def withDebugging(): SQLBase = this.copy(debug = true)
}

case class Intersect(
  left: SQLBase,
  right: SQLBase,
  debug: Boolean = false,
) extends CombinedQuery {

  override val operation = "intersect"

  override def bind[T](name: String, value: T): SQLBase = {
    this.copy(
      left = left.bind(name, value),
      right = right.bind(name, value)
    )
  }

  override def withDebugging(): SQLBase = this.copy(debug = true)
}

case class Except(
  left: SQLBase,
  right: SQLBase,
  debug: Boolean = false,
) extends CombinedQuery {

  override val operation = "except"

  override def bind[T](name: String, value: T): SQLBase = {
    this.copy(
      left = left.bind(name, value),
      right = right.bind(name, value)
    )
  }

  override def withDebugging(): SQLBase = this.copy(debug = true)
}

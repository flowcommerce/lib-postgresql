package io.flow.postgresql

import anorm.NamedParameter

sealed trait CombinedQuery extends SQLBase {
  protected def left: SQLBase
  protected def right: SQLBase
  protected def operation: String

  protected def verifyBoundVariables(): Unit = {
    val leftParams = left.namedParameters.map(_.name)
    val rightParams = right.namedParameters.map(_.name)
    val overlap = leftParams intersect rightParams
    assert(overlap.isEmpty, s"$operation has duplicate bound variables:\nvariables[$overlap]\nquery[${sql()}]")
  }

  override lazy val namedParameters: Seq[NamedParameter] =
    left.namedParameters ++ right.namedParameters

  override def sql(): String = s"(${left.sql()})\n$operation\n(${right.sql()})"

  override lazy val debuggingInfo: String = s"(${left.debuggingInfo})\n$operation\n(${right.debuggingInfo})"

  override lazy val interpolate: String = s"(${left.interpolate()})\n$operation\n(${right.interpolate()})"

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
  override val left: SQLBase,
  _right: SQLBase,
  debug: Boolean = false,
) extends CombinedQuery {

  override val right = _right.withReserved(left.namedParameters.map(_.name).toSet)

  override val operation = "union"

  verifyBoundVariables()

  override def bind[T](name: String, value: T): SQLBase = {
    this.copy(
      left = left.bind(name, value),
      _right = _right.bind(name, value)
    )
  }

  override def withReserved(reserved: Set[String]): SQLBase = {
    this.copy(
      left = left.withReserved(reserved),
      _right = right.withReserved(reserved),
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

  verifyBoundVariables()

  override def bind[T](name: String, value: T): SQLBase = {
    this.copy(
      left = left.bind(name, value),
      right = right.bind(name, value)
    )
  }

  override def withReserved(reserved: Set[String]): SQLBase =
    Intersect(left.withReserved(reserved), right.withReserved(reserved))

  override def withDebugging(): SQLBase = this.copy(debug = true)

}

case class Except(
  left: SQLBase,
  right: SQLBase,
  debug: Boolean = false,
) extends CombinedQuery {

  override val operation = "except"

  verifyBoundVariables()

  override def bind[T](name: String, value: T): SQLBase = {
    this.copy(
      left = left.bind(name, value),
      right = right.bind(name, value)
    )
  }

  override def withReserved(reserved: Set[String]): SQLBase =
    Except(left.withReserved(reserved), right.withReserved(reserved))

  override def withDebugging(): SQLBase = this.copy(debug = true)
}

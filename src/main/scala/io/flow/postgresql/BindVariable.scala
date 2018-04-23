package io.flow.postgresql

import java.util.UUID

import anorm.NamedParameter
import org.joda.time.{DateTime, LocalDate}

sealed trait BindVariable[T] extends Product with Serializable {

  def name: String
  def value: T
  def toNamedParameter: NamedParameter
  def defaultValueFunctions: Seq[Query.Function] = Nil
  def psqlType: Option[String] = None

  final def sqlPlaceholder: String = psqlType match {
    case None => s"{$name}"
    case Some(t) => s"{$name}::$t"
  }
}

object BindVariable {

  val DefaultBindName = "bind"

  /**
    * Creates a typed instances of a BindVariable for all types
    */
  def fromValue(name: String, value: Any): Option[BindVariable[_]] = {
    value match {
      case Some(v) => fromValue(name, v)
      case _: Unit | None => None
      // TODO: why is this not caught by Unit matcher above?
      case v if value.toString == "()" => None

      case v: UUID => Some(BindVariable.Uuid(name, v))
      case v: LocalDate => Some(BindVariable.DateVar(name, v))
      case v: DateTime => Some(BindVariable.DateTimeVar(name, v))
      case v: scala.Int => Some(BindVariable.Int(name, v))
      case v: Long => Some(BindVariable.BigInt(name, v))
      case v: Number => Some(BindVariable.Num(name, v))
      case v: String => Some(BindVariable.Str(name, v))
      case _ => Some(BindVariable.Str(name, value.toString))
    }
  }

  private[this] val LeadingUnderscores = """^_+""".r
  private[this] val MultiUnderscores = """__+""".r
  private[this] val TrailingUnderscores = """_+$""".r
  private[this] val ScrubName = """[^\w\d\_]""".r

  def safeName(name: String): String = {
    val idx = name.lastIndexOf(".")
    val simpleName = if (idx > 0) { name.substring(idx + 1) } else { name }.toLowerCase.trim

    val safeName = LeadingUnderscores.replaceAllIn(
      TrailingUnderscores.replaceAllIn(
        MultiUnderscores.replaceAllIn(
          ScrubName.replaceAllIn(simpleName.trim, "_"),
          "_"
        ),
        ""),
      ""
    )

    if (safeName.isEmpty) {
      DefaultBindName
    } else {
      safeName
    }
  }

  case class Int(override val name: String, override val value: scala.Int) extends BindVariable[scala.Int] {
    override val psqlType: Option[String] = Some("int")
    override def toNamedParameter: NamedParameter = NamedParameter(name, value.toString)
  }

  case class BigInt(override val name: String, override val value: Long) extends BindVariable[Long] {
    override val psqlType: Option[String] = Some("bigint")
    override def toNamedParameter: NamedParameter = NamedParameter(name, value.toString)
  }

  case class Num(override val name: String, override val value: Number) extends BindVariable[Number] {
    override val psqlType: Option[String] = Some("numeric")
    override def toNamedParameter: NamedParameter = NamedParameter(name, value.toString)
  }

  case class Str(override val name: String, override val value: String) extends BindVariable[String] {
    override val defaultValueFunctions: Seq[Query.Function] = Seq(Query.Function.Trim)
    override def toNamedParameter: NamedParameter = NamedParameter(name, value)
  }

  case class Uuid(override val name: String, override val value: UUID) extends BindVariable[UUID] {
    override val psqlType: Option[String] = Some("uuid")
    override def toNamedParameter: NamedParameter = NamedParameter(name, value.toString)
  }

  case class DateVar(override val name: String, override val value: LocalDate) extends BindVariable[LocalDate] {
    override val psqlType: Option[String] = Some("date")
    override def toNamedParameter: NamedParameter = NamedParameter(name, value.toString)
  }

  case class DateTimeVar(override val name: String, override val value: DateTime) extends BindVariable[DateTime] {
    override val psqlType: Option[String] = Some("timestamptz")
    override def toNamedParameter: NamedParameter = NamedParameter(name, value.toString)
  }

}

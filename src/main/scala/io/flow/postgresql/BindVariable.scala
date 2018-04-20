package io.flow.postgresql

import java.util.UUID

import anorm.NamedParameter
import org.joda.time.{DateTime, LocalDate}

sealed trait BindVariable {

  def name: String
  def defaultValueFunctions: Seq[Query.Function] = Nil
  def value: Any
  def toNamedParameter: NamedParameter
  def psqlType: Option[String]

  final def sql: String = psqlType match {
    case None => s"{$name}"
    case Some(t) => s"{$name}::$t"
  }
}

object BindVariable {

  case class Int(override val name: String, override val value: Number) extends BindVariable {
    override val psqlType: Option[String] = Some("int")
    override def toNamedParameter: NamedParameter = NamedParameter(name, value.toString)
  }

  case class BigInt(override val name: String, override val value: Number) extends BindVariable {
    override val psqlType: Option[String] = Some("bigint")
    override def toNamedParameter: NamedParameter = NamedParameter(name, value.toString)
  }

  case class Num(override val name: String, override val value: Number) extends BindVariable {
    override val psqlType: Option[String] = Some("numeric")
    override def toNamedParameter: NamedParameter = NamedParameter(name, value.toString)
  }

  case class Str(override val name: String, override val value: String) extends BindVariable {
    override val psqlType: Option[String] = None
    override val defaultValueFunctions: Seq[Query.Function] = Seq(Query.Function.Trim)
    override def toNamedParameter: NamedParameter = NamedParameter(name, value)
  }

  case class Uuid(override val name: String, override val value: UUID) extends BindVariable {
    override val psqlType: Option[String] = Some("uuid")
    override def toNamedParameter: NamedParameter = NamedParameter(name, value.toString)
  }

  case class DateVar(override val name: String, override val value: LocalDate) extends BindVariable {
    override val psqlType: Option[String] = Some("date")
    override def toNamedParameter: NamedParameter = NamedParameter(name, value.toString)
  }

  case class DateTimeVar(override val name: String, override val value: DateTime) extends BindVariable {
    override val psqlType: Option[String] = Some("timestamptz")
    override def toNamedParameter: NamedParameter = NamedParameter(name, value.toString)
  }

  case class Unit(override val name: String) extends BindVariable {
    override val psqlType: Option[String] = None
    override val value: Any = None
    override def toNamedParameter: NamedParameter = NamedParameter(name, Option.empty[String])
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

    safeName match {
      case "" => "bind"
      case _ => safeName
    }
  }

}
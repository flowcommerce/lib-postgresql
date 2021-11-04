package io.flow.postgresql

import java.util.UUID

import anorm.NamedParameter
import org.joda.time.{DateTime, LocalDate}

sealed trait BindVariable[T] extends Product with Serializable {

  def name: String
  def value: Option[T]
  def toNamedParameter: NamedParameter
  def defaultValueFunctions: Seq[Query.Function] = Nil
  def psqlType: Option[String] = None

  final def sqlPlaceholder: String = psqlType match {
    case None => s"{$name}"
    case Some(t) => s"{$name}::$t"
  }
}

sealed trait DefinedBindVariable[T] extends BindVariable[T] {
  val v: T
  override val value = Some(v)
}

object BindVariable {

  val DefaultBindName = "bind"

  /**
    * Creates a typed instances of a BindVariable for all types
    */
  def apply(name: String, value: Any): BindVariable[_] = {
    value match {
      case Some(v) => apply(name, v)
      case _: Unit | None => BindVariable.Unit(name)

      case v: UUID => BindVariable.Uuid(name, v)
      case v: LocalDate => BindVariable.DateVar(name, v)
      case v: DateTime => BindVariable.DateTimeVar(name, v)
      case v: scala.Int => BindVariable.Int(name, v)
      case v: Long => BindVariable.BigInt(name, v)
      case v: Number => BindVariable.Num(name, v)
      case v: String => BindVariable.Str(name, v)
      // TODO: why is this not caught by Unit matcher above?
      case v if v.toString == "()" => BindVariable.Unit(name)
      case _ => BindVariable.Str(name, value.toString)
    }
  }

  def safeName(name: String): String = {
    val idx = name.lastIndexOf(".")
    val simpleName = if (idx > 0) { name.substring(idx + 1) } else { name }

    val sb = new StringBuilder()
    simpleName.toLowerCase.trim.foreach { ch =>
      val ascii = ch.toInt
      // ASCII 0-9 or a-z
      if ((ascii >= 48 && ascii <= 57) || (ascii >= 97 && ascii <= 122)) {
        sb.append(ch)
      } else if (sb.length > 0 && sb.last != '_') {
        sb.append('_')
      }
    }

    val safeName = sb.toString()
    if (safeName.isEmpty) {
      DefaultBindName
    } else {
      if (safeName.endsWith("_")) safeName.substring(0, safeName.size - 1) else safeName
    }
  }

  case class Int(override val name: String, override val v: scala.Int) extends DefinedBindVariable[scala.Int] {
    override val psqlType: Option[String] = Some("int")
    override def toNamedParameter: NamedParameter = NamedParameter(name, v.toString)
  }

  case class BigInt(override val name: String, override val v: Long) extends DefinedBindVariable[Long] {
    override val psqlType: Option[String] = Some("bigint")
    override def toNamedParameter: NamedParameter = NamedParameter(name, v.toString)
  }

  case class Num(override val name: String, override val v: Number) extends DefinedBindVariable[Number] {
    override val psqlType: Option[String] = Some("numeric")
    override def toNamedParameter: NamedParameter = NamedParameter(name, v.toString)
  }

  case class Str(override val name: String, override val v: String) extends DefinedBindVariable[String] {
    override val defaultValueFunctions: Seq[Query.Function] = Seq(Query.Function.Trim)
    override def toNamedParameter: NamedParameter = NamedParameter(name, v)
  }

  case class Uuid(override val name: String, override val v: UUID) extends DefinedBindVariable[UUID] {
    override val psqlType: Option[String] = Some("uuid")
    override def toNamedParameter: NamedParameter = NamedParameter(name, v.toString)
  }

  case class DateVar(override val name: String, override val v: LocalDate) extends DefinedBindVariable[LocalDate] {
    override val psqlType: Option[String] = Some("date")
    override def toNamedParameter: NamedParameter = NamedParameter(name, v.toString)
  }

  case class DateTimeVar(override val name: String, override val v: DateTime) extends DefinedBindVariable[DateTime] {
    override val psqlType: Option[String] = Some("timestamptz")
    override def toNamedParameter: NamedParameter = NamedParameter(name, v.toString)
  }

  case class Unit(override val name: String) extends BindVariable[scala.Unit] {
    override def value: Option[scala.Unit] = None
    override def toNamedParameter: NamedParameter = NamedParameter(name, Option.empty[String])
  }

}

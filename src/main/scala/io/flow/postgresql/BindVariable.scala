package io.flow.postgresql

import java.util.UUID

import anorm.NamedParameter
import org.joda.time.{DateTime, LocalDate}

import scala.annotation.tailrec

/**
  * A container of bind variables used to generate unique,
  * readable names for each bind variables.
  */
case class BindVariables() {

  private[this] val internalVariables = scala.collection.mutable.ListBuffer[BindVariable[_]]()

  def variables(): Seq[BindVariable[_]] = internalVariables

  /**
    * Generates a unique bind variable name from the specified input
    *
    * @param name Preferred name of bind variable - will be used if unique,
    *             otherwise we generate a unique version.
    */
  def uniqueName(name: String): String = {
    uniqueName(name, 1)
  }

  @tailrec
  private[this] def uniqueName(original: String, count: Int): String = {
    assert(count >= 1)
    val scrubbedName = BindVariable.safeName(
      if (count == 1) { original } else { s"$original$count" }
    )

    if (internalVariables.exists(_.name == scrubbedName)) {
      uniqueName(original, count + 1)
    } else {
      scrubbedName
    }
  }

  def addWithUniqueName(name: String, value: Any): BindVariable[_] = {
    add(uniqueName(name), value)
  }

  /**
    * Creates a typed instances of a BindVariable for all types
    */
  def add(name: String, value: Any): BindVariable[_] = {
    val variable = value match {
      case v: UUID => BindVariable.Uuid(name, v)
      case v: LocalDate => BindVariable.DateVar(name, v)
      case v: DateTime => BindVariable.DateTimeVar(name, v)
      case v: Int => BindVariable.Int(name, v)
      case v: Long => BindVariable.BigInt(name, v)
      case v: Number => BindVariable.Num(name, v)
      case v: String => BindVariable.Str(name, v)
      case _: Unit => BindVariable.Unit(name)
      case _ => BindVariable.Str(name, value.toString)
    }
    internalVariables.append(variable)
    variable
  }
}
sealed trait BindVariable[T] extends Product with Serializable {

  def name: String
  def defaultValueFunctions: Seq[Query.Function] = Nil
  def value: T
  def toNamedParameter: NamedParameter
  def psqlType: Option[String]

  final def sql: String = psqlType match {
    case None => s"{$name}"
    case Some(t) => s"{$name}::$t"
  }
}

object BindVariable {

  private[this] val LeadingUnderscores = """^_+""".r
  private[this] val MultiUnderscores = """__+""".r
  private[this] val TrailingUnderscores = """_+$""".r
  private[this] val ScrubName = """[^\w\d\_]""".r
  private[this] val DefaultBindName = "bind"

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

  case class Int(override val name: String, override val value: _root_.scala.Int) extends BindVariable[_root_.scala.Int] {
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
    override val psqlType: Option[String] = None
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

  case class Unit(override val name: String) extends BindVariable[_root_.scala.Unit] {
    override val psqlType: Option[String] = None
    override val value: _root_.scala.Unit = ()
    override def toNamedParameter: NamedParameter = NamedParameter(name, Option.empty[String])
  }

}
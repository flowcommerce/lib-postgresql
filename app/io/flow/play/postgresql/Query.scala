package io.flow.play.postgresql

import anorm._
import java.util.UUID

trait BindVariable {

  def name: String
  def value: Any
  def toNamedParameter(): NamedParameter

}

object BindVariable {

  case class Str(override val name: String, override val value: String) extends BindVariable {
    assert(name == name.toLowerCase.trim, s"Bind variable[$name] must be lowercase and trimmed")
    override def toNamedParameter() = NamedParameter(name, value)
  }

  case class Num(override val name: String, override val value: Number) extends BindVariable {
    assert(name == name.toLowerCase.trim, s"Bind variable[$name] must be lowercase and trimmed")
    override def toNamedParameter() = NamedParameter(name, value.toString)
  }

  case class Uuid(override val name: String, override val value: UUID) extends BindVariable {
    assert(name == name.toLowerCase.trim, s"Bind variable[$name] must be lowercase and trimmed")
    override def toNamedParameter() = NamedParameter(name, value.toString)
  }

}

object Query {

  trait Function

  /**
   * Helper trait to allow caller to apply functions to the columns or
   * values in the query. A common use case is to apply
   * lower(text(...)) functions to enable case insensitive text
   * matching.
   */
  object Function {

    case object Lower extends Function {
      override def toString = "lower"
    }

    case object Trim extends Function {
      override def toString = "trim"
    }

    case class Custom(name: String) extends Function {
      override def toString = name
    }

  }

}

case class Query(
  base: String,
  conditions: Seq[String] = Nil,
  bind: Seq[BindVariable] = Nil,
  orderBy: Seq[String] = Nil,
  limit: Option[Long] = None,
  offset: Option[Long] = None
) {

  def multi[T](
    column: String,
    values: Option[Seq[T]]
  ): Query = {
    values match {
      case None => this
      case Some(v) => {
        v match {
          case Nil => {
            this.copy(
              conditions = conditions ++ Seq("false")
            )
          }
          case multiple => {
            val bindVariables = multiple.zipWithIndex.map { case (value, i) =>
              val n = if (i == 0) { column } else { s"${column}${i+1}" }
              toBindVariable(n, value)
            }
            val cond = s"$column in (%s)".format(bindVariables.map(_.name).mkString("{", "}, {", "}"))
            this.copy(
              conditions = conditions ++ Seq(cond),
              bind = bind ++ bindVariables
            )
          }
        }
      }
    }
  }

  def condition(
    value: Option[String]
  ): Query = {
    value match {
      case None => this
      case Some(v) => this.copy(conditions = conditions ++ Seq(v))
    }
  }

  def bind[T](
    name: String,
    value: Option[T]
  ): Query = {
    value match {
      case None => this
      case Some(v) => {
        bind.find(_.name == name) match {
          case None => {
            this.copy(bind = bind ++ Seq(BindVariable.Str(name, v.toString)))
          }
          case Some(_) => {
            sys.error(s"Bind variable named '$name' already defined")
          }
        }
      }
    }
  }

  def number[Number](
    column: String,
    value: Option[Number]
  ): Query = {
    value match {
      case None => this
      case Some(v) => {
        val bindVar = toBindVariable(column, v)

        this.copy(
          conditions = conditions ++ Seq(s"$column = {${bindVar.name}}"),
          bind = bind ++ Seq(bindVar)
        )
      }
    }
  }

  /**
    * 
    * @param subquery Accepts the name of the bind variable and
    *        returns a string representing the subquery
    */
  def subquery[T](
    column: String,
    bindVarName: String,
    value: Option[T],
    subqueryGenerator: String => String
  ): Query = {
    value match {
      case None => this
      case Some(v) => {
        val bindVar = toBindVariable(bindVarName, v)
        val condition = s"$column in (" + subqueryGenerator(bindVar.name) + ")"

        this.copy(
          conditions = conditions ++ Seq(condition),
          bind = bind ++ Seq(bindVar)
        )
      }
    }
  }

  def uuid[T](
    column: String,
    value: Option[T]
  ): Query = {
    value match {
      case None => this
      case Some(v) => {
        val bindVar = toBindVariable(column, v)
        this.copy(
          conditions = conditions ++ Seq(
            s"$column = {${bindVar.name}}::uuid"
          ),
          bind = bind ++ Seq(bindVar)
        )
      }
    }
  }

  def text[T](
    column: String,
    value: Option[T],
    columnFunctions: Seq[Query.Function] = Nil,
    valueFunctions: Seq[Query.Function] = Seq(Query.Function.Trim)
  ): Query = {
    value match {
      case None => this
      case Some(v) => {
        val bindVar = toBindVariable(column, v)

        val exprColumn = withFunctions(column, columnFunctions)
        val exprValue = withFunctions(s"{${bindVar.name}}", valueFunctions)

        this.copy(
          conditions = conditions ++ Seq(
            s"$exprColumn = $exprValue"
          ),
          bind = bind ++ Seq(bindVar)
        )
      }
    }
  }

  def boolean(column: String, value: Option[Boolean]): Query = {
    value match {
      case None => this
      case Some(v) => {
        val cond = v match {
          case true => s"$column is true"
          case false => s"$column is false"
        }

        this.copy(conditions = conditions ++ Seq(cond))
      }
    }
  }

  def isNull(column: String): Query = {
    this.copy(
      conditions = conditions ++ Seq(s"$column is null")
    )
  }

  def isNotNull(column: String): Query = {
    this.copy(
      conditions = conditions ++ Seq(s"$column is not null")
    )
  }

  def nullBoolean(column: String, value: Option[Boolean]): Query = {
    value match {
      case None => this
      case Some(v) => {
        val cond = v match {
          case true => s"$column is not null"
          case false => s"$column is null"
        }
        this.copy(
          conditions = conditions ++ Seq(cond)
        )
      }
    }
  }

  def orderBy(value: Option[String]): Query = {
    value match {
      case None => this
      case Some(clause) => {
        this.copy(orderBy = orderBy ++ Seq(clause))
      }
    }
  }

  def limit(value: Option[Long]): Query = {
    value match {
      case None => this
      case Some(v) => {
        this.copy(limit = Some(v))
      }
    }
  }

  def offset(value: Option[Long]): Query = {
    value match {
      case None => this
      case Some(v) => {
        this.copy(offset = Some(v))
      }
    }
  }

  /**
    * Creates the full text of the sql query
    */
  def sql(): String = {
    val query = conditions match {
      case Nil => base
      case conds => {
        base + " where " + conds.mkString(" and ")
      }
    }

    Seq(
      Some(query),
      orderBy match {
        case Nil => None
        case clauses => Some("order by " + clauses.mkString(", "))
      },
      limit.map(v => s"limit $v"),
      offset.map(v => s"offset $v")
    ).flatten.mkString(" ")
  }

  /**
   * Useful only for debugging. Returns the sql query with all bind
   * variables interpolated for easy inspection.
   */
  def interpolate(): String = {
    bind.foldLeft(sql()) { case (query, bindVar) =>
      query.replace(s"{${bindVar.name}}", s"'${bindVar.value}'")
    }
  }

  def as[T](
    parser: anorm.ResultSetParser[T]
  ) (
    implicit c: java.sql.Connection
  ) = {
    anormSql().as(parser)
  }

  /**
    * Prepares the sql query for anorm, including any bind variables.
    */
  def anormSql(): anorm.SimpleSql[anorm.Row] = {
    SQL(sql).on(bind.map(_.toNamedParameter): _*)
  }


  /**
    * Generates a unique, as friendly as possible, bind variable name
    */
  private[this] def toBindVariable(column: String, value: Any): BindVariable = {
    val idx = column.lastIndexOf(".")
    val simpleName = if (idx > 0) { column.substring(idx + 1) } else { column }.toLowerCase
    val name = bind.find(_.name == simpleName) match {
      case Some(_) => s"${simpleName}${bind.size + 1}"
      case None => simpleName
    }
    value match {
      case v: UUID => BindVariable.Uuid(name.toLowerCase.trim, v)
      case v: Number => BindVariable.Num(name.toLowerCase.trim, v)
      case v: String => BindVariable.Str(name.toLowerCase.trim, v)
      case _ => BindVariable.Str(name.toLowerCase.trim, value.toString)
    }
  }

  private[this] def withFunctions(name: String, options: Seq[Query.Function]): String = {
    options.reverse.foldLeft(name) { case (value, option) =>
      s"${option}($value)"
    }
  }

}

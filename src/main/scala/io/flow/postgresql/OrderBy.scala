package io.flow.postgresql

case class OrderBy(clauses: Seq[String]) {

  def sql: Option[String] = clauses match {
    case Nil => None
    case multiple => Some(multiple.mkString(", "))
  }

}

object OrderBy {

  val ValidFunctions = Seq("lower", "json")

  def parse(
    value: String,
    defaultTable: Option[String] = None
  ): Either[Seq[String], OrderBy] = {
    value.trim.split(",").map(_.trim).filter(!_.isEmpty).toList match {
      case Nil => {
        Right(OrderBy(Nil))
      }
      case clauses => {
        clauses.find { c =>
          c.indexOf(" ") >= 0
        } match {
          case None => {
            val parsed: Seq[Either[String, Clause]] = clauses.map { parseDirection(_, defaultTable) }
            parsed.collect {case Left(x) => x } match {
              case Nil => Right(
                OrderBy(
                  parsed.collect {case Right(x) => x }.map(_.sql)
                )
              )
              case errors => Left(errors)
            }
          }
          case Some(invalid) => {
            Left(
              Seq(
                s"Sort[$invalid] contained a space which is not allowed"
              )
            )
          }
        }
      }
    }
  }

  def apply(
    value: String,
    defaultTable: Option[String] = None
  ): OrderBy = {
    parse(value, defaultTable) match {
      case Left(errors) => sys.error(s"Error parsing[$value]: " + errors.mkString(", "))
      case Right(orderBy) => orderBy
    }
  }

  private[this] def parseDirection(
    value: String,
    tableName: Option[String]
  ): Either[String, Clause] = {
    if (value.startsWith("-")) {
      parseFunction(Direction.Desc, value.substring(1), tableName)
    } else if (value.startsWith("+")) {
      parseFunction(Direction.Asc, value.substring(1), tableName)
    } else {
      parseFunction(Direction.Asc, value, tableName)
    }
  }

  // Look for funtions
  private[this] val FunctionRegexp = """^([a-z]+)\((.+)\)$""".r

  private[this] def parseFunction(
    direction: Direction,
    value: String,
    tableName: Option[String]
  ): Either[String, Clause] = {
    value match {
      case FunctionRegexp(function, column) => {
        ValidFunctions.contains(function) match {
          case false => Left(s"$value: Invalid function[$function]. Must be one of: " + ValidFunctions.mkString(", "))
          case true =>
            function match {
              case "json" =>
                withJson(column) match {
                  case Right(query) => Right(Clause(direction, query))
                  case Left(error) => Left(error)
                }
              case _ => Right(Clause(direction, withTable(column, tableName), function = Some(function)))
            }
        }
      }
      case _ => {
        Right(
          Clause(direction, withTable(value, tableName))
        )
      }
    }
  }

  private[this] def withTable(
    column: String,
    tableName: Option[String]
  ): String = {
    tableName match {
      case None => column
      case Some(table) => {
        if (column.indexOf(".") > 0) {
          column
        } else {
          s"${table}.$column"
        }
      }
    }
  }

  private[this] def withJson(
   path: String
  ): Either[String, String] = {
    path.split("\\.") match {
      case Array(column, parent, field) => Right(s"(${column.toString}->>'${parent.toString}')::jsonb->'${field.toString}'")
      case Array(column, field) => Right(s"$column->>'$field'")
      case _ =>  Left(s"Error defining json query column[$path]: Must be column.field[.field]")
    }
  }

  private[this] sealed trait Direction
  private[this] object Direction {
    case object Asc extends Direction
    case object Desc extends Direction
  }

  private case class Clause(
    direction: Direction,
    column: String,
    function: Option[String] = None
  ) {
    val sql = {
      val text = function match {
        case None => column
        case Some(f) => s"$f($column)"
      }
      direction match {
        case Direction.Asc => text
        case Direction.Desc => s"$text desc"
      }
    }
  }
}

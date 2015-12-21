package db

case class OrderBy(clauses: Seq[String]) {

  def sql: Option[String] = clauses match {
    case Nil => None
    case multiple => Some(multiple.mkString(", "))
  }

}

object OrderBy {

  def parse(
    value: String,
    defaultTable: Option[String] = None
  ): Either[Seq[Error], OrderBy] = {
    Right(
      OrderBy(
        value.split(",").map(_.trim).filter(!_.isEmpty).map { parseDirection(_, defaultTable) }.map(_.sql)
      )
    )
  }

  def parseOrError(
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
  ): Clause = {
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
  ): Clause = {
    value match {
      case FunctionRegexp(function, column) => {
        Clause(direction, withTable(column, tableName), function = Some(function))
      }
      case _ => {
        Clause(direction, withTable(value, tableName))
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

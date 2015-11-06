package io.flow.play.postgresql

sealed trait Direction
object Direction {

  case object Ascending extends Direction { override def toString() = "asc" }
  case object Descending extends Direction { override def toString() = "desc" }

  val all = Seq(Ascending, Descending)

  def fromString(value: String): Option[Direction] = all.find(value == _.toString)

}

case class OrderBy(table: String, column: String, direction: Direction) {

  private[this] val dict = Map(
    "roles" -> Seq("name")
  )

  // TODO: Link up data dictionary
  private[this] def isText(): Boolean = {
    dict.lift(table) match {
      case None => false
      case Some(columns) => columns.contains(column)
    }
  }

  override def toString(): String = {
    val main = isText() match {
      case true => s"lower($table.$column)"
      case false => s"$table.$column"
    }

    direction match {
      case Direction.Descending => s"$main desc"
      case Direction.Ascending => main
    }
  }

}

object OrderBy {

  def asc(table: String, column: String) = OrderBy(table, column, Direction.Ascending)
  def desc(table: String, column: String) = OrderBy(table, column, Direction.Descending)

}

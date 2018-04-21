package io.flow.postgresql

import scala.annotation.tailrec

sealed trait QueryCondition

object QueryCondition {

  case class Column[T](
    column: String,
    operator: String,
    values: Seq[T],
    columnFunctions: Seq[Query.Function] = Nil,
    valueFunctions: Seq[Query.Function] = Nil
  ) extends QueryCondition {

    def bind(reservedKeys: Set[String]): BoundQueryCondition.Column = {
      val allocated = scala.collection.mutable.Set[String]()
      BoundQueryCondition.Column(
        column = column,
        operator = operator,
        columnFunctions = columnFunctions,
        valueFunctions = valueFunctions,
        variables = values.map { value =>
          val name = uniqueName(reservedKeys ++ allocated, column)
          allocated.add(name)
          BindVariable(name, value)
        }
      )
    }

  }

  case class Static(expression: String) extends QueryCondition

  case class Subquery(column: String, query: Query) extends QueryCondition {
    def bind(reservedKeys: Set[String]): BoundQueryCondition.Subquery = {
      BoundQueryCondition.Subquery(
        column,
        query.copy(
          explicitBindVariables = query.explicitBindVariables ++ reservedKeys.map { key =>
            BindVariable(key, ()) // Reserve the keys previously allocated
          }
        )
      )
    }
  }

  /**
    * Generates a unique bind variable name from the specified input
    *
    * @param original Preferred name of bind variable - will be used if unique,
    *                 otherwise we generate a unique version.
    */
  @tailrec
  private[this] def uniqueName(reservedKeys: Set[String], original: String, count: Int = 1): String = {
    assert(count >= 1)
    val scrubbedName = BindVariable.safeName(
      if (count == 1) { original } else { s"$original$count" }
    )

    if (reservedKeys.contains(scrubbedName)) {
      uniqueName(reservedKeys, original, count + 1)
    } else {
      scrubbedName
    }
  }
}

sealed trait BoundQueryCondition

object BoundQueryCondition {

  case class Column(
    column: String,
    operator: String,
    variables: Seq[BindVariable[_]],
    columnFunctions: Seq[Query.Function] = Nil,
    valueFunctions: Seq[Query.Function] = Nil
  ) extends BoundQueryCondition

  case class Static(expression: String) extends BoundQueryCondition

  case class Subquery(column: String, query: Query) extends BoundQueryCondition

}

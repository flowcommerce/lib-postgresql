package io.flow.postgresql

import anorm._
import org.joda.time.{DateTime, LocalDate}

import java.util.UUID
import scala.annotation.tailrec

object Query {

  trait Function

  /** Helper trait to allow caller to apply functions to the columns or values in the query. A common use case is to
    * apply lower(text(...)) functions to enable case insensitive text matching.
    */
  object Function {

    case object Lower extends Function {
      override def toString: String = "lower"
    }

    case object Upper extends Function {
      override def toString: String = "upper"
    }

    case object Trim extends Function {
      override def toString: String = "trim"
    }

  }

}

case class Query(
  base: String,
  conditions: Seq[QueryCondition] = Nil,
  orderBy: Seq[String] = Nil,
  limit: Option[Long] = None,
  offset: Option[Long] = None,
  debug: Boolean = false,
  groupBy: Seq[String] = Nil,
  having: Option[String] = None,
  locking: Option[String] = None,
  explicitBindVariables: Seq[BindVariable[_]] = Nil,
  reservedBindVariableNames: Set[String] = Set.empty
) {

  private[this] lazy val boundConditions: Seq[BoundQueryCondition] = {
    resolveBoundConditions(
      reservedKeys = explicitBindVariables.map(_.name).toSet ++ reservedBindVariableNames,
      remaining = conditions,
      resolved = Nil
    )
  }

  /** Recursively bind all the variables. Primary use case is to make sure all subquery bind variables are namespaces
    * properly
    */
  private[this] def resolveBoundConditions(
    reservedKeys: Set[String],
    remaining: Seq[QueryCondition],
    resolved: Seq[BoundQueryCondition]
  ): Seq[BoundQueryCondition] = {
    remaining.toList match {
      case Nil => resolved
      case one :: rest => {
        one match {
          case c: QueryCondition.Column[_] => {
            val newCondition = c.bind(reservedKeys)
            resolveBoundConditions(
              reservedKeys = reservedKeys ++ newCondition.variables.map(_.name).toSet,
              remaining = rest,
              resolved = resolved ++ Seq(newCondition)
            )
          }

          case c: QueryCondition.Columns[_] =>
            val newCondition = c.bind(reservedKeys)
            resolveBoundConditions(
              reservedKeys = reservedKeys ++ newCondition.variables.flatMap(_.map(_.name)).toSet,
              remaining = rest,
              resolved = resolved ++ Seq(newCondition)
            )

          case QueryCondition.Not(cond) => {
            val not = resolveBoundConditions(reservedKeys, Seq(cond), Nil).toList match {
              case c :: Nil => c
              case other => sys.error(s"Expected 1 bound condition but found[${other.size}]: $cond")
            }

            resolveBoundConditions(
              reservedKeys = reservedKeys ++ findKeys(Seq(not)),
              remaining = rest,
              resolved = resolved ++ Seq(
                BoundQueryCondition.Not(not)
              )
            )
          }

          case QueryCondition.Static(expression) => {
            resolveBoundConditions(
              reservedKeys = reservedKeys,
              remaining = rest,
              resolved = resolved ++ Seq(BoundQueryCondition.Static(expression))
            )
          }

          case c: QueryCondition.Subquery => {
            val subquery = c.bind(reservedKeys)
            resolveBoundConditions(
              reservedKeys = reservedKeys ++ subquery.query.allBindVariables.map(_.name).toSet,
              remaining = rest,
              resolved = resolved ++ Seq(subquery)
            )
          }

          case c: QueryCondition.OrClause => {
            val or = resolveBoundConditions(reservedKeys, c.conditions, Nil)
            resolveBoundConditions(
              reservedKeys = reservedKeys ++ findKeys(or),
              remaining = rest,
              resolved = resolved ++ Seq(
                BoundQueryCondition.OrClause(or)
              )
            )
          }
        }
      }
    }
  }

  private[this] def findKeys(
    conditions: Seq[BoundQueryCondition],
    found: Set[String] = Set.empty
  ): Set[String] = {
    conditions.toList match {
      case Nil => found
      case first :: rest => {
        val newKeys = first match {
          case c: BoundQueryCondition.OrClause => findKeys(c.conditions)
          case c: BoundQueryCondition.Not => findKeys(Seq(c.condition))
          case _: BoundQueryCondition.Static => Nil
          case c: BoundQueryCondition.Column => c.variables.map(_.name).toSet
          case c: BoundQueryCondition.Columns => c.variables.flatMap(_.map(_.name)).toSet
          case c: BoundQueryCondition.Subquery => c.query.allBindVariables.map(_.name).toSet
        }
        findKeys(rest, found ++ newKeys)
      }
    }
  }

  private[postgresql] lazy val allBindVariables: Seq[BindVariable[_]] = {
    explicitBindVariables ++ boundConditions.flatMap {
      case c: BoundQueryCondition.Column => c.variables
      case c: BoundQueryCondition.Columns => c.variables.flatten
      case BoundQueryCondition.Static(_) => Nil
      case c: BoundQueryCondition.Subquery => c.query.allBindVariables
      case c: BoundQueryCondition.Not => {
        c.condition match {
          case _: BoundQueryCondition.OrClause => sys.error("Recursive or resolution not supported")
          case _: BoundQueryCondition.Not => sys.error("Recursive not resolution not supported")
          case _: BoundQueryCondition.Static => Nil
          case c: BoundQueryCondition.Column => c.variables
          case c: BoundQueryCondition.Columns => c.variables.flatten
          case c: BoundQueryCondition.Subquery => c.query.allBindVariables
        }
      }
      case c: BoundQueryCondition.OrClause =>
        c.conditions.flatMap {
          case _: BoundQueryCondition.OrClause => sys.error("Recursive or resolution not supported")
          case _: BoundQueryCondition.Not => sys.error("Recursive not resolution not supported")
          case _: BoundQueryCondition.Static => Nil
          case c: BoundQueryCondition.Column => c.variables
          case c: BoundQueryCondition.Columns => c.variables.flatten
          case c: BoundQueryCondition.Subquery => c.query.allBindVariables
        }
    }
  }

  def equals[T](column: String, value: Option[T]): Query = optionalOperation(column, "=", value)
  def equals[T](column: String, value: T): Query = operation(column, "=", value)

  def equalsIgnoreCase[T](column: String, value: Option[T]): Query = optionalOperation(
    column,
    "=",
    value,
    columnFunctions = Seq(Query.Function.Lower, Query.Function.Trim),
    valueFunctions = Seq(Query.Function.Lower, Query.Function.Trim)
  )
  def equalsIgnoreCase[T](column: String, value: T): Query = operation(
    column,
    "=",
    value,
    columnFunctions = Seq(Query.Function.Lower, Query.Function.Trim),
    valueFunctions = Seq(Query.Function.Lower, Query.Function.Trim)
  )

  def notEquals[T](column: String, value: Option[T]): Query = optionalOperation(column, "!=", value)
  def notEquals[T](column: String, value: T): Query = operation(column, "!=", value)

  def notEqualsIgnoreCase[T](column: String, value: Option[T]): Query = optionalOperation(
    column,
    "!=",
    value,
    columnFunctions = Seq(Query.Function.Lower, Query.Function.Trim),
    valueFunctions = Seq(Query.Function.Lower, Query.Function.Trim)
  )
  def notEqualsIgnoreCase[T](column: String, value: T): Query = operation(
    column,
    "!=",
    value,
    columnFunctions = Seq(Query.Function.Lower, Query.Function.Trim),
    valueFunctions = Seq(Query.Function.Lower, Query.Function.Trim)
  )

  def lessThan[T](column: String, value: Option[T]): Query = optionalOperation(column, "<", value)
  def lessThan[T](column: String, value: T): Query = operation(column, "<", value)

  def lessThanOrEquals[T](column: String, value: Option[T]): Query = optionalOperation(column, "<=", value)
  def lessThanOrEquals[T](column: String, value: T): Query = operation(column, "<=", value)

  def greaterThan[T](column: String, value: Option[T]): Query = optionalOperation(column, ">", value)
  def greaterThan[T](column: String, value: T): Query = operation(column, ">", value)

  def greaterThanOrEquals[T](column: String, value: Option[T]): Query = optionalOperation(column, ">=", value)
  def greaterThanOrEquals[T](column: String, value: T): Query = operation(column, ">=", value)

  def operation[T](
    column: String,
    operator: String,
    value: T,
    columnFunctions: Seq[Query.Function] = Nil,
    valueFunctions: Seq[Query.Function] = Nil
  ): Query = {
    val condition = value match {
      case q: Query => {
        QueryCondition.Subquery(
          column = column,
          operator = operator,
          query = q,
          columnFunctions = columnFunctions
        )
      }
      case _ => {
        QueryCondition.Column(
          column = column,
          operator = operator,
          values = Seq(value),
          columnFunctions = columnFunctions,
          valueFunctions = valueFunctions
        )
      }
    }
    if (this.conditions.contains(condition)) {
      // exact condition has already been added - no reason to readd
      this
    } else {
      this.copy(
        conditions = conditions ++ Seq(condition)
      )
    }
  }

  def optionalOperation[T](
    column: String,
    operator: String,
    value: Option[T],
    columnFunctions: Seq[Query.Function] = Nil,
    valueFunctions: Seq[Query.Function] = Nil
  ): Query = {
    value match {
      case None => this
      case Some(v) => operation(column, operator, v, columnFunctions, valueFunctions)
    }
  }

  def optionalIn[T](
    column: String,
    values: Option[Seq[T]],
    columnFunctions: Seq[Query.Function] = Nil,
    valueFunctions: Seq[Query.Function] = Nil
  ): Query = {
    values match {
      case None => this
      case Some(v) => in(column, v, columnFunctions, valueFunctions)
    }
  }

  def optionalInMulti[T](
    columns: Seq[String],
    values: Option[Seq[Seq[T]]],
    columnFunctions: Seq[Seq[Query.Function]] = Nil,
    valueFunctions: Seq[Seq[Query.Function]] = Nil
  ): Query =
    values match {
      case None => this
      case Some(v) => inMulti(columns, v, columnFunctions, valueFunctions)
    }

  def optionalIn2[T1, T2](
    columns: (String, String),
    values: Option[Seq[(T1, T2)]],
    columnFunctions: (Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil),
    valueFunctions: (Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil)
  ): Query =
    values match {
      case None => this
      case Some(v) => in2(columns, v, columnFunctions, valueFunctions)
    }

  def optionalIn3[T1, T2, T3](
    columns: (String, String, String),
    values: Option[Seq[(T1, T2, T3)]],
    columnFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil, Nil),
    valueFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil, Nil)
  ): Query =
    values match {
      case None => this
      case Some(v) => in3(columns, v, columnFunctions, valueFunctions)
    }

  def optionalIn4[T1, T2, T3, T4](
    columns: (String, String, String, String),
    values: Option[Seq[(T1, T2, T3, T4)]],
    columnFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (
      Nil,
      Nil,
      Nil,
      Nil
    ),
    valueFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (
      Nil,
      Nil,
      Nil,
      Nil
    )
  ): Query =
    values match {
      case None => this
      case Some(v) => in4(columns, v, columnFunctions, valueFunctions)
    }

  def optionalIn5[T1, T2, T3, T4, T5](
    columns: (String, String, String, String, String),
    values: Option[Seq[(T1, T2, T3, T4, T5)]],
    columnFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (
      Nil,
      Nil,
      Nil,
      Nil,
      Nil
    ),
    valueFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (
      Nil,
      Nil,
      Nil,
      Nil,
      Nil
    )
  ): Query =
    values match {
      case None => this
      case Some(v) => in5(columns, v, columnFunctions, valueFunctions)
    }

  def optionalIn6[T1, T2, T3, T4, T5, T6](
    columns: (String, String, String, String, String, String),
    values: Option[Seq[(T1, T2, T3, T4, T5, T6)]],
    columnFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (
      Nil,
      Nil,
      Nil,
      Nil,
      Nil,
      Nil
    ),
    valueFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (
      Nil,
      Nil,
      Nil,
      Nil,
      Nil,
      Nil
    )
  ): Query =
    values match {
      case None => this
      case Some(v) => in6(columns, v, columnFunctions, valueFunctions)
    }

  def in(column: String, query: Query): Query =
    addSubquery(column, query, "in")

  def notIn(column: String, query: Query): Query =
    addSubquery(column, query, "not in")

  private[this] def addSubquery(column: String, query: Query, operator: String): Query =
    this.copy(
      conditions = conditions ++ Seq(
        QueryCondition.Subquery(
          column = column,
          operator = operator,
          query = query
        )
      )
    )

  def in[T](
    column: String,
    values: Seq[T],
    columnFunctions: Seq[Query.Function] = Nil,
    valueFunctions: Seq[Query.Function] = Nil
  ): Query = {
    inClauseBuilder("in", column, values, columnFunctions, valueFunctions)
  }

  def inMulti[T](
    columns: Seq[String],
    values: Seq[Seq[T]],
    columnFunctions: Seq[Seq[Query.Function]] = Nil,
    valueFunctions: Seq[Seq[Query.Function]] = Nil
  ): Query =
    inMultipleClauseBuilder("in", columns, values, columnFunctions, valueFunctions)

  def in2[T1, T2](
    columns: (String, String),
    values: Seq[(T1, T2)],
    columnFunctions: (Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil),
    valueFunctions: (Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil)
  ): Query =
    inMultipleClauseBuilder(
      "in",
      Seq(columns._1, columns._2),
      values.map { case (v1, v2) => Seq[Any](v1, v2) },
      Seq(columnFunctions._1, columnFunctions._2),
      Seq(valueFunctions._1, valueFunctions._2)
    )

  def in3[T1, T2, T3](
    columns: (String, String, String),
    values: Seq[(T1, T2, T3)],
    columnFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil, Nil),
    valueFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil, Nil)
  ): Query =
    inMultipleClauseBuilder(
      "in",
      Seq(columns._1, columns._2, columns._3),
      values.map { case (v1, v2, v3) => Seq[Any](v1, v2, v3) },
      Seq(columnFunctions._1, columnFunctions._2, columnFunctions._3),
      Seq(valueFunctions._1, valueFunctions._2, valueFunctions._3)
    )

  def in4[T1, T2, T3, T4](
    columns: (String, String, String, String),
    values: Seq[(T1, T2, T3, T4)],
    columnFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) =
      (Nil, Nil, Nil, Nil),
    valueFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) =
      (Nil, Nil, Nil, Nil)
  ): Query =
    inMultipleClauseBuilder(
      "in",
      Seq(columns._1, columns._2, columns._3, columns._4),
      values.map { case (v1, v2, v3, v4) => Seq[Any](v1, v2, v3, v4) },
      Seq(columnFunctions._1, columnFunctions._2, columnFunctions._3, columnFunctions._4),
      Seq(valueFunctions._1, valueFunctions._2, valueFunctions._3, valueFunctions._4)
    )

  def in5[T1, T2, T3, T4, T5](
    columns: (String, String, String, String, String),
    values: Seq[(T1, T2, T3, T4, T5)],
    columnFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (Nil, Nil, Nil, Nil, Nil),
    valueFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (Nil, Nil, Nil, Nil, Nil)
  ): Query =
    inMultipleClauseBuilder(
      "in",
      Seq(columns._1, columns._2, columns._3, columns._4, columns._5),
      values.map { case (v1, v2, v3, v4, v5) => Seq[Any](v1, v2, v3, v4, v5) },
      Seq(columnFunctions._1, columnFunctions._2, columnFunctions._3, columnFunctions._4, columnFunctions._5),
      Seq(valueFunctions._1, valueFunctions._2, valueFunctions._3, valueFunctions._4, valueFunctions._5)
    )

  def in6[T1, T2, T3, T4, T5, T6](
    columns: (String, String, String, String, String, String),
    values: Seq[(T1, T2, T3, T4, T5, T6)],
    columnFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (Nil, Nil, Nil, Nil, Nil, Nil),
    valueFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (Nil, Nil, Nil, Nil, Nil, Nil)
  ): Query =
    inMultipleClauseBuilder(
      "in",
      Seq(columns._1, columns._2, columns._3, columns._4, columns._5, columns._6),
      values.map { case (v1, v2, v3, v4, v5, v6) => Seq[Any](v1, v2, v3, v4, v5, v6) },
      Seq(
        columnFunctions._1,
        columnFunctions._2,
        columnFunctions._3,
        columnFunctions._4,
        columnFunctions._5,
        columnFunctions._6
      ),
      Seq(
        valueFunctions._1,
        valueFunctions._2,
        valueFunctions._3,
        valueFunctions._4,
        valueFunctions._5,
        valueFunctions._6
      )
    )

  def optionalNotIn[T](
    column: String,
    values: Option[Seq[T]],
    columnFunctions: Seq[Query.Function] = Nil,
    valueFunctions: Seq[Query.Function] = Nil
  ): Query = {
    values match {
      case None => this
      case Some(v) => notIn(column, v, columnFunctions, valueFunctions)
    }
  }

  def optionalNotInMulti[T](
    columns: Seq[String],
    values: Option[Seq[Seq[T]]],
    columnFunctions: Seq[Seq[Query.Function]] = Nil,
    valueFunctions: Seq[Seq[Query.Function]] = Nil
  ): Query =
    values match {
      case None => this
      case Some(v) => notInMulti(columns, v, columnFunctions, valueFunctions)
    }

  def optionalNotIn2[T1, T2](
    columns: (String, String),
    values: Option[Seq[(T1, T2)]],
    columnFunctions: (Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil),
    valueFunctions: (Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil)
  ): Query =
    values match {
      case None => this
      case Some(v) => notIn2(columns, v, columnFunctions, valueFunctions)
    }

  def optionalNotIn3[T1, T2, T3](
    columns: (String, String, String),
    values: Option[Seq[(T1, T2, T3)]],
    columnFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil, Nil),
    valueFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil, Nil)
  ): Query =
    values match {
      case None => this
      case Some(v) => notIn3(columns, v, columnFunctions, valueFunctions)
    }

  def optionalNotIn4[T1, T2, T3, T4](
    columns: (String, String, String, String),
    values: Option[Seq[(T1, T2, T3, T4)]],
    columnFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (
      Nil,
      Nil,
      Nil,
      Nil
    ),
    valueFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (
      Nil,
      Nil,
      Nil,
      Nil
    )
  ): Query =
    values match {
      case None => this
      case Some(v) => notIn4(columns, v, columnFunctions, valueFunctions)
    }

  def optionalNotIn5[T1, T2, T3, T4, T5](
    columns: (String, String, String, String, String),
    values: Option[Seq[(T1, T2, T3, T4, T5)]],
    columnFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (
      Nil,
      Nil,
      Nil,
      Nil,
      Nil
    ),
    valueFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (
      Nil,
      Nil,
      Nil,
      Nil,
      Nil
    )
  ): Query =
    values match {
      case None => this
      case Some(v) => notIn5(columns, v, columnFunctions, valueFunctions)
    }

  def optionalNotIn6[T1, T2, T3, T4, T5, T6](
    columns: (String, String, String, String, String, String),
    values: Option[Seq[(T1, T2, T3, T4, T5, T6)]],
    columnFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (
      Nil,
      Nil,
      Nil,
      Nil,
      Nil,
      Nil
    ),
    valueFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (
      Nil,
      Nil,
      Nil,
      Nil,
      Nil,
      Nil
    )
  ): Query =
    values match {
      case None => this
      case Some(v) => notIn6(columns, v, columnFunctions, valueFunctions)
    }

  def notIn[T](
    column: String,
    values: Seq[T],
    columnFunctions: Seq[Query.Function] = Nil,
    valueFunctions: Seq[Query.Function] = Nil
  ): Query =
    inClauseBuilder("not in", column, values, columnFunctions, valueFunctions)

  def notInMulti[T](
    columns: Seq[String],
    values: Seq[Seq[T]],
    columnFunctions: Seq[Seq[Query.Function]] = Nil,
    valueFunctions: Seq[Seq[Query.Function]] = Nil
  ): Query =
    inMultipleClauseBuilder("not in", columns, values, columnFunctions, valueFunctions)

  def notIn2[T1, T2](
    columns: (String, String),
    values: Seq[(T1, T2)],
    columnFunctions: (Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil),
    valueFunctions: (Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil)
  ): Query =
    inMultipleClauseBuilder(
      "not in",
      Seq(columns._1, columns._2),
      values.map { case (v1, v2) => Seq[Any](v1, v2) },
      Seq(columnFunctions._1, columnFunctions._2),
      Seq(valueFunctions._1, valueFunctions._2)
    )

  def notIn3[T1, T2, T3](
    columns: (String, String, String),
    values: Seq[(T1, T2, T3)],
    columnFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil, Nil),
    valueFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (Nil, Nil, Nil)
  ): Query =
    inMultipleClauseBuilder(
      "not in",
      Seq(columns._1, columns._2, columns._3),
      values.map { case (v1, v2, v3) => Seq[Any](v1, v2, v3) },
      Seq(columnFunctions._1, columnFunctions._2, columnFunctions._3),
      Seq(valueFunctions._1, valueFunctions._2, valueFunctions._3)
    )

  def notIn4[T1, T2, T3, T4](
    columns: (String, String, String, String),
    values: Seq[(T1, T2, T3, T4)],
    columnFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (
      Nil,
      Nil,
      Nil,
      Nil
    ),
    valueFunctions: (Seq[Query.Function], Seq[Query.Function], Seq[Query.Function], Seq[Query.Function]) = (
      Nil,
      Nil,
      Nil,
      Nil
    )
  ): Query =
    inMultipleClauseBuilder(
      "not in",
      Seq(columns._1, columns._2, columns._3, columns._4),
      values.map { case (v1, v2, v3, v4) => Seq[Any](v1, v2, v3, v4) },
      Seq(columnFunctions._1, columnFunctions._2, columnFunctions._3, columnFunctions._4),
      Seq(valueFunctions._1, valueFunctions._2, valueFunctions._3, valueFunctions._4)
    )

  def notIn5[T1, T2, T3, T4, T5](
    columns: (String, String, String, String, String),
    values: Seq[(T1, T2, T3, T4, T5)],
    columnFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (
      Nil,
      Nil,
      Nil,
      Nil,
      Nil
    ),
    valueFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (
      Nil,
      Nil,
      Nil,
      Nil,
      Nil
    )
  ): Query =
    inMultipleClauseBuilder(
      "not in",
      Seq(columns._1, columns._2, columns._3, columns._4, columns._5),
      values.map { case (v1, v2, v3, v4, v5) => Seq[Any](v1, v2, v3, v4, v5) },
      Seq(columnFunctions._1, columnFunctions._2, columnFunctions._3, columnFunctions._4, columnFunctions._5),
      Seq(valueFunctions._1, valueFunctions._2, valueFunctions._3, valueFunctions._4, valueFunctions._5)
    )

  def notIn6[T1, T2, T3, T4, T5, T6](
    columns: (String, String, String, String, String, String),
    values: Seq[(T1, T2, T3, T4, T5, T6)],
    columnFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (
      Nil,
      Nil,
      Nil,
      Nil,
      Nil,
      Nil
    ),
    valueFunctions: (
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function],
      Seq[Query.Function]
    ) = (
      Nil,
      Nil,
      Nil,
      Nil,
      Nil,
      Nil
    )
  ): Query =
    inMultipleClauseBuilder(
      "not in",
      Seq(columns._1, columns._2, columns._3, columns._4, columns._5, columns._6),
      values.map { case (v1, v2, v3, v4, v5, v6) => Seq[Any](v1, v2, v3, v4, v5, v6) },
      Seq(
        columnFunctions._1,
        columnFunctions._2,
        columnFunctions._3,
        columnFunctions._4,
        columnFunctions._5,
        columnFunctions._6
      ),
      Seq(
        valueFunctions._1,
        valueFunctions._2,
        valueFunctions._3,
        valueFunctions._4,
        valueFunctions._5,
        valueFunctions._6
      )
    )

  private[this] def inClauseBuilder[T](
    operator: String,
    column: String,
    values: Seq[T],
    columnFunctions: Seq[Query.Function],
    valueFunctions: Seq[Query.Function]
  ): Query = {
    assert(
      operator == "in" || operator == "not in",
      s"Invalid operation[$operator] - must be 'in' or 'not in'"
    )

    this.copy(
      conditions = conditions ++ Seq(
        QueryCondition.Column(
          column = column,
          operator = operator,
          values = values,
          columnFunctions = columnFunctions,
          valueFunctions = valueFunctions
        )
      )
    )
  }

  private[this] def inMultipleClauseBuilder[T](
    operator: String,
    columns: Seq[String],
    values: Seq[Seq[T]],
    columnFunctions: Seq[Seq[Query.Function]],
    valueFunctions: Seq[Seq[Query.Function]]
  ): Query = {
    assert(
      operator == "in" || operator == "not in",
      s"Invalid operation[$operator] - must be 'in' or 'not in'"
    )
    values.zipWithIndex.find { case (vs, _) => vs.size != columns.size }.map { case (vs, i) => (i, vs.size) } match {
      case Some((i, size)) =>
        val msg = s"Invalid number of values at index $i: columns.size = ${columns.size} but values[$i].size = $size." +
          s" You must provide as many values as columns."
        throw new java.lang.AssertionError(msg)
      case None =>
        this.copy(
          conditions = conditions ++ Seq(
            QueryCondition.Columns(
              columns = columns,
              operator = operator,
              values = values,
              columnFunctions = columnFunctions,
              valueFunctions = valueFunctions
            )
          )
        )
    }
  }

  def or(
    clauses: Seq[String]
  ): Query = {
    clauses.distinct match {
      case Nil => this
      case one :: Nil => and(maybeWrapInParens(one))
      case multiple => and("(" + multiple.mkString(" or ") + ")")
    }
  }

  private[this] def maybeWrapInParens(value: String): String = {
    def hasSpace(v: String) = v.contains(" ")

    // special case the simple clauses like 'id=5'
    value.split("=").toList.map(_.trim) match {
      case a :: Nil if !hasSpace(a) => value
      case a :: b :: Nil if !hasSpace(a) && !hasSpace(b) => value
      case _ => s"($value)"
    }
  }

  def or(
    clause: Option[String]
  ): Query = {
    or(clause.toSeq)
  }

  def or(
    clause: String
  ): Query = {
    or(Seq(clause))
  }

  def orClause(
    clause: QueryCondition*
  ): Query = {
    this.copy(conditions = conditions ++ Seq(QueryCondition.OrClause(clause)))
  }

  def not(
    clause: QueryCondition
  ): Query = {
    this.copy(conditions = conditions ++ Seq(QueryCondition.Not(clause)))
  }

  def and(
    clauses: Seq[String]
  ): Query = {
    this.copy(conditions = conditions ++ clauses.distinct.map(QueryCondition.Static))
  }

  def and(
    clause: String
  ): Query = {
    and(Seq(clause))
  }

  def and(
    clause: Option[String]
  ): Query = {
    clause match {
      case None => this
      case Some(v) => and(v)
    }
  }

  /** Adds a bind variable to this query. You will receive a runtime error if this bind variable is already defined.
    */
  def bind[T](
    name: String,
    value: T
  ): Query = {
    val safe = BindVariable.safeName(name)
    assert(
      safe == name.toLowerCase.trim,
      s"Invalid bind variable name[$name]" + (
        if (safe == BindVariable.DefaultBindName) { "" }
        else { s" suggest: $safe" }
      )
    )
    assert(
      !explicitBindVariables.exists { bv => BindVariable.safeName(bv.name) == safe },
      s"Bind variable named '$name' already defined"
    )
    assert(
      !reservedBindVariableNames.contains(safe),
      s"Bind variable named '$name' has been reserved and cannot be used again in this query"
    )
    this.copy(
      explicitBindVariables = explicitBindVariables ++ Seq(BindVariable(name, value))
    )
  }

  def bind[T](
    name: String,
    value: Option[T]
  ): Query = {
    value match {
      case None => bind(name, ())
      case Some(v) => bind(name, v)
    }
  }

  def optionalText[T](
    column: String,
    value: Option[T],
    columnFunctions: Seq[Query.Function] = Nil,
    valueFunctions: Seq[Query.Function] = Seq(Query.Function.Trim)
  ): Query = {
    optionalOperation(
      column = column,
      operator = "=",
      value = value,
      columnFunctions = columnFunctions,
      valueFunctions = valueFunctions
    )
  }

  def text[T](
    column: String,
    value: T,
    columnFunctions: Seq[Query.Function] = Nil,
    valueFunctions: Seq[Query.Function] = Seq(Query.Function.Trim)
  ): Query = {
    operation(
      column = column,
      operator = "=",
      value = value,
      columnFunctions = columnFunctions,
      valueFunctions = valueFunctions
    )
  }

  def isTrue(column: String): Query = boolean(column, value = true)
  def isFalse(column: String): Query = boolean(column, value = false)

  def boolean(column: String, value: Option[Boolean]): Query = {
    value match {
      case None => this
      case Some(v) => boolean(column, v)
    }
  }

  def boolean(column: String, value: Boolean): Query = {
    and(
      if (value) {
        s"$column is true"
      } else {
        s"$column is false"
      }
    )
  }

  def isNull(column: String): Query = {
    and(s"$column is null")
  }

  def isNotNull(column: String): Query = {
    and(s"$column is not null")
  }

  def nullBoolean(column: String, value: Option[Boolean]): Query = {
    value match {
      case None => this
      case Some(v) => nullBoolean(column, v)
    }
  }

  def nullBoolean(column: String, value: Boolean): Query = {
    and(
      if (value) {
        s"$column is not null"
      } else {
        s"$column is null"
      }
    )
  }

  def groupBy(value: Option[String]): Query = {
    value match {
      case None => this
      case Some(v) => groupBy(v)
    }
  }

  def groupBy(value: String): Query = {
    this.copy(groupBy = groupBy ++ Seq(value))
  }

  def having(value: Option[String]): Query = {
    this.copy(having = value)
  }

  def having(value: String): Query = {
    having(Some(value))
  }

  def orderBy(value: Option[String]): Query = {
    value match {
      case None => this
      case Some(v) => orderBy(v)
    }
  }

  def orderBy(value: String): Query = {
    this.copy(orderBy = orderBy ++ Seq(value))
  }

  def optionalLimit(value: Option[Long]): Query = {
    value match {
      case None => this
      case Some(v) => limit(v)
    }
  }

  def limit(value: Long): Query = {
    this.copy(limit = Some(value))
  }

  def optionalOffset(value: Option[Long]): Query = {
    value match {
      case None => this
      case Some(v) => offset(v)
    }
  }

  def offset(value: Long): Query = {
    this.copy(offset = Some(value))
  }

  def locking(clause: String): Query = copy(locking = Some(clause))

  /** Creates the full text of the sql query
    */
  def sql(): String = {
    val query = boundConditions match {
      case Nil => base
      case conds => {
        base + " where " + conds.map(toSql).mkString(" and ")
      }
    }

    Seq(
      Some(query),
      groupBy match {
        case Nil => None
        case clauses => Some("group by " + clauses.mkString(", "))
      },
      having.map(h => s"having $h"),
      orderBy match {
        case Nil => None
        case clauses => Some("order by " + clauses.mkString(", "))
      },
      limit.map(v => s"limit $v"),
      offset.map(v => s"offset $v"),
      locking
    ).flatten.mkString(" ")
  }

  /** Turns on debugging of this query.
    */
  def withDebugging(): Query = {
    this.copy(
      debug = true
    )
  }

  /** Useful only for debugging. Returns the sql query with all bind variables interpolated for easy inspection.
    */
  def interpolate(): String = {
    allBindVariables.foldLeft(sql()) { case (query, bindVar) =>
      bindVar match {
        case BindVariable.Int(name, value) => {
          query.replace(bindVar.sqlPlaceholder, value.toString).replace(s"{$name}", value.toString)
        }
        case BindVariable.BigInt(name, value) => {
          query.replace(bindVar.sqlPlaceholder, value.toString).replace(s"{$name}", value.toString)
        }
        case BindVariable.Num(name, value) => {
          query.replace(bindVar.sqlPlaceholder, value.toString).replace(s"{$name}", value.toString)
        }
        case BindVariable.Uuid(_, value) => {
          query.replace(bindVar.sqlPlaceholder, s"'$value'::uuid")
        }
        case BindVariable.DateVar(_, value) => {
          query.replace(bindVar.sqlPlaceholder, s"'$value'::date")
        }
        case BindVariable.DateTimeVar(_, value) => {
          query.replace(bindVar.sqlPlaceholder, s"'$value'::timestamptz")
        }
        case BindVariable.Str(_, value) => {
          query.replace(bindVar.sqlPlaceholder, s"'$value'")
        }
        case BindVariable.Unit(_) => {
          query.replace(bindVar.sqlPlaceholder, "null")
        }
      }
    }
  }

  private[this] def toSql(condition: BoundQueryCondition): String = {
    condition match {
      case c: BoundQueryCondition.Column => {
        c.variables.toList match {
          case Nil => "false" // Intentionally match no rows on empty list

          case bindVar :: Nil if c.operator != "in" && c.operator != "not in" => {
            val exprColumn = withFunctions(c.column, c.columnFunctions, bindVar.value)
            val exprValue =
              withFunctions(bindVar.sqlPlaceholder, c.valueFunctions ++ bindVar.defaultValueFunctions, bindVar.value)
            s"$exprColumn ${c.operator} $exprValue"
          }

          case multiple => {
            val exprColumn = withFunctions(c.column, c.columnFunctions, multiple.headOption.map(_.value))

            s"$exprColumn ${c.operator} (%s)".format(
              multiple
                .map { bindVar =>
                  withFunctions(
                    bindVar.sqlPlaceholder,
                    c.valueFunctions ++ bindVar.defaultValueFunctions,
                    multiple.head
                  )
                }
                .mkString(", ")
            )
          }
        }
      }

      case c: BoundQueryCondition.Columns =>
        c.variables.toList match {
          case Nil => "false" // Intentionally match no rows on empty list

          case bindVar :: Nil if c.operator != "in" && c.operator != "not in" => {
            val exprColumn = withFunctionsMultiple(c.columns, c.columnFunctions, bindVar.map(_.value))
            val functions =
              bindVar
                .map(_.defaultValueFunctions)
                .zipAll[Seq[Query.Function], Seq[Query.Function]](c.valueFunctions, Seq.empty, Seq.empty)
                .map { case (df, vf) => vf ++ df }
            val exprValue =
              withFunctionsMultiple(bindVar.map(_.sqlPlaceholder), functions, bindVar.map(_.value))
            s"$exprColumn ${c.operator} $exprValue"
          }

          case multiple =>
            val values = multiple.head.map(_.value)
            val exprColumn = withFunctionsMultiple(c.columns, c.columnFunctions, values)
            s"$exprColumn ${c.operator} (%s)".format(
              multiple
                .map { bindVar =>
                  val functions =
                    bindVar
                      .map(_.defaultValueFunctions)
                      .zipAll[Seq[Query.Function], Seq[Query.Function]](c.valueFunctions, Seq.empty, Seq.empty)
                      .map { case (df, vf) => vf ++ df }
                  withFunctionsMultiple(bindVar.map(_.sqlPlaceholder), functions, values)
                }
                .mkString(", ")
            )
        }

      case BoundQueryCondition.Static(expression) => expression

      case c: BoundQueryCondition.Subquery => {
        val exprColumn = withFunctions(c.column, c.columnFunctions, "TODO")
        s"$exprColumn ${c.operator} (${c.query.sql()})"
      }

      case c: BoundQueryCondition.OrClause => {
        c.conditions.map(toSql).toList match {
          case one :: Nil => one
          case other => "(" + other.mkString(" or ") + ")"
        }
      }

      case c: BoundQueryCondition.Not => {
        "not (" + toSql(c.condition) + ")"
      }
    }
  }

  def as[T](
    parser: anorm.ResultSetParser[T]
  )(implicit
    c: java.sql.Connection
  ): T = {
    anormSql().as(parser)
  }

  /** Returns debugging information about this query
    */
  def debuggingInfo(): String = {
    if (allBindVariables.isEmpty) {
      interpolate()
    } else {
      Seq(
        sql(),
        allBindVariables
          .map { bv =>
            s" - ${bv.name}: ${bv.value.getOrElse("null")}"
          }
          .mkString("\n"),
        "Interpolated:",
        interpolate()
      ).mkString("\n")
    }
  }

  /** Prepares the sql query for anorm, including any bind variables.
    */
  def anormSql(): anorm.SimpleSql[anorm.Row] = {
    if (debug) {
      println(debuggingInfo())
    }
    SQL(sql()).on(allBindVariables.map(_.toNamedParameter): _*)
  }

  private[this] def withFunctions[T](
    name: String,
    functions: Seq[Query.Function],
    value: T
  ): String =
    applicableFunctions(functions, value).distinct.reverse.foldLeft(name) { case (acc, function) => s"$function($acc)" }

  /** Apply functions to the column or value in the query. An example use case is to apply (lower(trim(n1)),
    * lower(trim(n2))) to enable case insensitive text matching.
    */
  private[this] def withFunctionsMultiple[T](
    names: Seq[String],
    functions: Seq[Seq[Query.Function]],
    values: Seq[T]
  ): String =
    names
      .zip(values)
      .map(Some(_))
      .zipAll(functions, Option.empty[(String, T)], Seq.empty)
      .collect { case (Some((n, v)), fs) => (n, v, fs) }
      .map { case (n, v, fs) => withFunctions(n, fs, v) }
      .mkString("(", ", ", ")")

  /** Doesn't makes sense to apply lower/trim on all types. select only applicable filters based on the type of the
    * value
    */
  @tailrec
  private[this] def applicableFunctions[T](
    functions: Seq[Query.Function],
    value: T
  ): Seq[Query.Function] =
    value match {
      case None | _: UUID | _: LocalDate | _: DateTime | _: Int | _: Long | _: Number | _: Unit => Nil
      case Some(v) => applicableFunctions(functions, v)
      case _ => functions
    }

}

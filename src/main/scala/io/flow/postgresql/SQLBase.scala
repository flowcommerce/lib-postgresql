package io.flow.postgresql

import anorm.{NamedParameter, SQL}

trait SQLBase {
  val debug: Boolean

  def sql(): String

  def namedParameters: Seq[NamedParameter]

  private[postgresql] def withReserved(reserved: Set[String]): SQLBase

  def union(query: SQLBase) = Union(this, query)

  def intersect(query: SQLBase) = Intersect(this, query)

  def except(query: SQLBase) = Except(this, query)

  def debuggingInfo: String

  def withDebugging(): SQLBase

  def interpolate(): String

  def as[T](
    parser: anorm.ResultSetParser[T]
  ) (
    implicit c: java.sql.Connection
  ): T = {
    anormSql().as(parser)
  }

  /**
    * Prepares the sql query for anorm, including any bind variables.
    */
  def anormSql(): anorm.SimpleSql[anorm.Row] = {
    if (debug) {
      println(debuggingInfo)
    }
    SQL(sql()).on(namedParameters: _*)
  }

  /**
    * Adds a bind variable to this query. You will receive a runtime
    * error if this bind variable is already defined.
    */
  def bind[T](name: String, value: T): SQLBase
}

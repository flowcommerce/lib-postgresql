package io.flow.postgresql

import java.sql.Connection

import anorm.BatchSql

import scala.util.{Failure, Success, Try}

object BatchQuery {
  // a Query object used only to collect bind variables
  val BindQuery: Query = Query("batch")
}

/**
  * Simple interface to use the Query bind API to create a BatchSql
  * anorm instance to allow you to write multiple records in DB with
  * one DB call.
  *
  * Example:
  * TestDatabase.withConnection { implicit c =>
  *   BatchQueryBuilder(UpsertUserQuery)
  *     .withRow { r => r.bind("id", "1").bind("name", name1) }
  *     .withRow { r => r.bind("id", "2").bind("name", name2) }
  *     .execute()
  * }
  */
case class BatchQueryBuilder(
  query: Query,
  rows: Seq[Seq[BindVariable[_]]] = Nil,
) {

  def withRow(f: Query => Query): BatchQueryBuilder = {
    this.copy(
      rows = rows ++ Seq(f(BatchQuery.BindQuery).allBindVariables)
    )
  }

  def execute(implicit c: Connection): Array[Int] = {
    if (rows.isEmpty) {
      Array.empty
    } else {
      Try {
        build().execute()(c)
      } match {
        case Success(r) => r
        case Failure(ex) => {
          throw new RuntimeException(
            s"Error executing batch sql query: ${ex.getMessage}\n${debuggingInfo()}",
            ex
          )
        }
      }
    }
  }

  def build(): BatchSql = {
    if (query.debug) {
      println(debuggingInfo())
      println("\n")
    }
    assert(
      rows.nonEmpty,
      "Must have at least one row to execute a batch sql query"
    )
    val parentBindVars = query.allBindVariables.map(_.toNamedParameter)
      BatchSql(
      query.sql(),
        parentBindVars ++ rows.head.map(_.toNamedParameter),
        rows.drop(1).map { r => parentBindVars ++ r.map(_.toNamedParameter) }: _*
    )
  }

  /**
    * Returns debugging information about this query
    */
  def debuggingInfo(): String = {
    Seq(
      "Batch SQL Query",
      query.sql(),
      rows.zipWithIndex.map { case (r, i) =>
        (Seq(s"Row ${i+1}:") ++ r.map { bv =>
          s" - ${bv.name}: ${bv.value.getOrElse("null")}"
        }).mkString("\n")
      }.toList match {
        case Nil => " - no rows specified"
        case data => data.mkString("\n")
      }

    ).mkString("\n")
  }
}

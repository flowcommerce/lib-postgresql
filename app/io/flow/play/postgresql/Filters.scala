package io.flow.play.postgresql

import java.util.UUID

object Filters {

  def multipleGuids(column: String, values: Seq[UUID]): String = {
    values match {
      case Nil => {
        "and false"
      }
      case multiple => {
        // TODO: Bind vars
        s"and $column in (" + multiple.map( v => s"'$v'::uuid" ).mkString(", ") + ")"
      }
    }
  }

  def isDeleted(
    tableName: String,
    value: Boolean
  ): String = {
    value match {
      case true => s"and $tableName.deleted_at is not null"
      case false => s"and $tableName.deleted_at is null"
    }
  }

  def isExpired(
    tableName: String,
    value: Boolean
  ): String = {
    value match {
      case true => { s"and $tableName.expires_at < now()" }
      case false => { s"and $tableName.expires_at >= now()" }
    }
  }

}

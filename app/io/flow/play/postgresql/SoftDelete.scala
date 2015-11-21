package io.flow.play.postgresql

import anorm._
import play.api.db._
import play.api.Play.current
import java.util.UUID

/**
  * Basic helpers to build sql queries for "soft deleting" records (by
  * updating deleted_at and deleted_by_guid columns)
  */
object SoftDelete {

  def delete(tableName: String, deletedBy: UUID, guid: String) {
    delete(tableName, deletedBy, UUID.fromString(guid))
  }

  def delete(c: java.sql.Connection, tableName: String, deletedBy: UUID, guid: String) {
    delete(c, tableName, deletedBy, UUID.fromString(guid))
  }

  def delete(tableName: String, deletedBy: UUID, guid: UUID): Unit = {
    delete(tableName, deletedBy, ("guid", Some("::uuid"), guid.toString))
  }

  def delete(c: java.sql.Connection, tableName: String, deletedBy: UUID, guid: UUID): Unit = {
    delete(c, tableName, deletedBy, ("guid", Some("::uuid"), guid.toString))
  }

  def delete(tableName: String, deletedBy: UUID, field: (String, Option[String], String)) {
    DB.withConnection { implicit c =>
      delete(c, tableName, deletedBy, field)
    }
  }

  def delete(c: java.sql.Connection, tableName: String, deletedBy: UUID, field: (String, Option[String], String)) {
    val (name, tpe, value) = field
    val SoftDeleteQuery = s"""
      update %s set deleted_by_guid = {deleted_by_guid}::uuid, deleted_at = now() where ${name} = {${name}}${tpe.getOrElse("")} and deleted_at is null
    """
    DB.withConnection { implicit c =>
      SQL(SoftDeleteQuery.format(tableName)).on('deleted_by_guid -> deletedBy, name -> value).execute()
    }
  }

}

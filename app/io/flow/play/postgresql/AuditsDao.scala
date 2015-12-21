package io.flow.play.postgresql

import io.flow.common.v0.models.{Audit, Reference}
import org.joda.time.DateTime
import java.util.UUID

object AuditsDao {

  def all(
    tableName: String,
    prefix: Option[String] = None
  ) = {
    val p = prefix.getOrElse(tableName) + "_audit"
    Seq(
      s"$tableName.created_at as ${p}_created_at",
      s"$tableName.created_by_guid as ${p}_created_by_guid",
      s"$tableName.updated_at as ${p}_updated_at",
      s"$tableName.updated_by_guid as ${p}_updated_by_guid"
    ).mkString(", ")
  }

  def creationOnly(
    tableName: String,
    prefix: Option[String] = None
  ) = {
    val p = prefix.getOrElse(tableName) + "_audit"
    Seq(
      s"$tableName.created_at as ${p}_created_at",
      s"$tableName.created_by_guid as ${p}_created_by_guid",
      s"$tableName.created_at as ${p}_updated_at",
      s"$tableName.created_by_guid as ${p}_updated_by_guid"
    ).mkString(", ")
  }

  def fromRow(
    row: anorm.Row,
    prefix: Option[String] = None,
    sep: Option[String] = Some("_")
  ): Audit = {
    val p = prefix.map( _ + sep.getOrElse("")).getOrElse("")
    Audit(
      createdAt = row[DateTime](s"${p}created_at"),
      createdBy = Reference(
        guid = row[UUID](s"${p}created_by_guid")
      ),
      updatedAt = row[DateTime](s"${p}updated_at"),
      updatedBy = Reference(
        guid = row[UUID](s"${p}updated_by_guid")
      )
    )
  }

}

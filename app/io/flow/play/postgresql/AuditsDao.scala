package io.flow.play.postgresql

import io.flow.common.v0.models.{Audit, Reference}
import anorm._
import anorm.JodaParameterMetaData._
import org.joda.time.DateTime
import java.util.UUID

object AuditsDao {

  def query(tableName: String) = {
    Seq(
      queryCreation(tableName),
      s"${tableName}.updated_at",
      s"${tableName}.updated_by_guid"
    ).mkString(", ")
  }

  def queryCreation(tableName: String) = {
    Seq(
      s"${tableName}.created_at",
      s"${tableName}.created_by_guid"
    ).mkString(", ")
  }

  def queryWithAlias(tableName: String, prefix: String) = {
    Seq(
      s"${tableName}.created_at as ${prefix}_created_at",
      s"${tableName}.created_by_guid as ${prefix}_created_by_guid",
      s"${tableName}.updated_at as ${prefix}_updated_at",
      s"${tableName}.updated_by_guid as ${prefix}_updated_by_guid"
    ).mkString(", ")
  }

  def queryCreationWithAlias(tableName: String, prefix: String) = {
    Seq(
      s"${tableName}.created_at as ${prefix}_created_at",
      s"${tableName}.created_by_guid as ${prefix}_created_by_guid"
    ).mkString(", ")
  }

  /**
   * Creates an instance of an audit object from the underlying row,
   * expecting columns named 'created_at', 'created_by_guid',
   * 'updated_at', 'updated_by_guid'
   */
  def fromRow(
    row: anorm.Row,
    prefix: Option[String] = None
  ): Audit = {
    val p = prefix.map( _ + "_").getOrElse("")
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

  /**
   * Creates an instance of an audit object from creation data only
   * (updated information will be defaulted to the creation info)
   */
  def fromRowCreation(
    row: anorm.Row,
    prefix: Option[String] = None
  ): Audit = {
    val p = prefix.map( _ + "_").getOrElse("")

    val createdAt = row[DateTime](s"${p}created_at")
    val createdBy = Reference(
      guid = row[UUID](s"${p}created_by_guid")
    )

    Audit(
      createdAt = createdAt,
      createdBy = createdBy,
      updatedAt = createdAt,
      updatedBy = createdBy
    )
  }

}

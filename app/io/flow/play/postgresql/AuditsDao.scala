package io.flow.play.postgresql

object AuditsDao {

  def all(tableName: String) = {
    val prefix = s"${tableName}_audit"
    Seq(
      s"$tableName.created_at as ${prefix}_created_at",
      s"$tableName.created_by_guid as ${prefix}_created_by_guid",
      s"$tableName.updated_at as ${prefix}_updated_at",
      s"$tableName.updated_by_guid as ${prefix}_updated_by_guid"
    ).mkString(", ")
  }

  def creationOnly(tableName: String) = {
    val prefix = s"${tableName}_audit"
    Seq(
      s"$tableName.created_at as ${prefix}_created_at",
      s"$tableName.created_by_guid as ${prefix}_created_by_guid",
      s"$tableName.created_at as ${prefix}_updated_at",
      s"$tableName.created_by_guid as ${prefix}_updated_by_guid"
    ).mkString(", ")
  }

}

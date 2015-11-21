package io.flow.play.postgresql

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

}

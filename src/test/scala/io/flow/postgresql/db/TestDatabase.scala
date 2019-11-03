package io.flow.postgresql.db

import java.sql.{Connection, DriverManager}

object TestDatabase {
  Class.forName("org.postgresql.Driver")

  private[this] val DbUrl = "jdbc:postgresql://localhost:5432/libpostgresqldb"
  private[this] val DbUsername = "api"
  private[this] val DbPassword = ""

  def withConnection[T](f: Connection => T): T = {
    val c = DriverManager.getConnection(DbUrl, DbUsername, DbPassword)
    try {
      f(c)
    } finally {
      c.close()
    }
  }
}

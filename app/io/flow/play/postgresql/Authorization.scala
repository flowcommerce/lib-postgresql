package io.flow.play.postgresql

import java.util.UUID

sealed trait Authorization {
  case object PublicOnly extends Authorization
  case object All extends Authorization
  case class User(guid: UUID) extends Authorization
  case class Organization(key: String) extends Authorization
}

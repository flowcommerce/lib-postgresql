package io.flow.play.postgresql

import java.util.UUID

sealed trait Authorization

object Authorization {

  case object PublicOnly extends Authorization
  case object All extends Authorization
  case class User(guid: UUID) extends Authorization
  case class Organization(key: String) extends Authorization

  def fromUser(userGuid: Option[UUID]): Authorization = {
    userGuid match {
      case None => Authorization.PublicOnly
      case Some(guid) => Authorization.User(guid)
    }
  }

  def fromOrganization(orgId: Option[String]): Authorization = {
    orgId match {
      case None => Authorization.PublicOnly
      case Some(id) => Authorization.Organization(id)
    }
  }
  
}

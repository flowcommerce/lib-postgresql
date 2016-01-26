package io.flow.postgresql

sealed trait Authorization

object Authorization {

  case object PublicOnly extends Authorization
  case object All extends Authorization
  case class User(id: String) extends Authorization
  case class Organization(id: String) extends Authorization

  def fromUser(userId: Option[String]): Authorization = {
    userId match {
      case None => Authorization.PublicOnly
      case Some(id) => Authorization.User(id)
    }
  }

  def fromOrganization(orgId: Option[String]): Authorization = {
    orgId match {
      case None => Authorization.PublicOnly
      case Some(id) => Authorization.Organization(id)
    }
  }
  
}

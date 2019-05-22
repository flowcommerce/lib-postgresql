package io.flow.postgresql

sealed trait Authorization

object Authorization {

  case object PublicOnly extends Authorization
  case object All extends Authorization
  case class User(id: String) extends Authorization
  case class Organization(id: String) extends Authorization
  case class Partner(id: String) extends Authorization
  case class Session(id: String) extends Authorization
  case class Customer(number: String, sessionId: String) extends Authorization

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

  def fromPartner(partnerId: Option[String]): Authorization = {
    partnerId match {
      case None => Authorization.PublicOnly
      case Some(id) => Authorization.Partner(id)
    }
  }

  def fromSession(sessionId: Option[String]): Authorization = {
    sessionId match {
      case None => Authorization.PublicOnly
      case Some(id) => Authorization.Session(id)
    }
  }

  def fromCustomer(customerNumber: Option[String], sessionId: Option[String]): Authorization = {
    (customerNumber, sessionId) match {
      case (Some(cn), Some(sid)) => Authorization.Customer(cn, sid)
      case (None, Some(sid)) => Authorization.Session(sid)
      case _ => Authorization.PublicOnly
    }
  }

}

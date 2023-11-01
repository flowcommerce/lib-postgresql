package io.flow.postgresql

object Sanitize {

  val ValidCharacters: Set[String] =
    "_-,.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".split("").toSet

  /** Returns true IFF the string contains only safe characters
    */
  def isSafe(value: String): Boolean = {
    value.trim.split("").forall(ValidCharacters.contains)
  }

}

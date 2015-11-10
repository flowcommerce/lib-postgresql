package io.flow.play.postgresql

object Pager {

  /**
    * Iterator that takes two functions:
    *   pagerFunction: Method to return a page of results
    *   perObjectFunction: Function to call on each element
    * 
    * Example:
    *   Pager.eachPage { offset =>
    *     ProjectsDao.findAll(offset = offset)
    *   } { project =>
    *     ProjectActor.sync(project)
    *   }
    */
  def eachPage[T](
    pagerFunction: Int => Iterable[T]
  ) (
    perObjectFunction: T => Unit
  ) {
    var offset = 0
    var haveMore = true

    while (haveMore) {
      val objects = pagerFunction(offset)
      haveMore = !objects.isEmpty
      offset += objects.size
      objects.foreach { perObjectFunction(_) }
    }
  }

}

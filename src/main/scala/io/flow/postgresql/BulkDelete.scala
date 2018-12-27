package io.flow.postgresql

object BulkDelete {

  /**
    * Helper to delete a large set of records, one page at at
    * time. You provide a function to return records to delete; this
    * method will iterate recursively until there are no more records.
    * 
    * Example:
    * 
    *    BulkDelete.byPage {
    *      subscriptionsDao.findAll(
    *        publication = Some(Publication.DailySummary)
    *      )
    *    } { subscription =>
    *      subscriptionsDao.delete(Constants.SystemUser, subscription)
    *    }
    */
  @scala.annotation.tailrec
  final def byPage[T](
    f: => Iterable[T]
  ) (
    deleteFunction: T => Unit
  ): Unit = {
    val results = f

    results.foreach { r =>
      deleteFunction(r)
    }

    if (f.nonEmpty) {
      byPage(f)(deleteFunction)
    }
  }

}

package io.flow.postgresql

object Pager {

  /**
    * Helper to create a pager, inferring the types from the function
    * returning each page of results.
    *
    * Example:
    *
    *    Pager.create { offset =>
    *      SubscriptionsDao.findAll(
    * publication = Some(Publication.DailySummary),
    * offset = offset
    * )
    * }.foreach { subscription =>
    * println("subscription: " + subscription)
    * }
    */
  def create[T](
    f: Long => Iterable[T]
  ): Pager[T] = {
    new Pager[T] {
      override def page(offset: Long): Iterable[T] = f(offset)
    }
  }
}

object DeletionPager {
  /**
    * Helper to create a deletion pager to paginate over objects that are being deleted.
    * An offset cannot be used as the number of pages may vary depending on object deletions.
    * Object types are inferred from the function returning each page of results.
    *
    * Example:
    *
    *    DeletionPager.create {
    *      SubscriptionsDao.findAll(
    *        publication = Some(Publication.DailySummary)
    *      )
    *    }.foreach { subscription =>
    *      delete(deletedBy, subscription)
    *    }
    */
  def create[T](
    f: Iterable[T]
  ): DeletionPager[T] = {
    new DeletionPager[T] {
      override def page: Iterable[T] = f
    }
  }

}


/**
  * Trait that enables us to iterate over a large number of results
  * (e.g. from a database query) one page at a time.
  */
trait Pager[T] extends Iterator[T] {

  private[this] var nextResult: Option[T] = None
  private[this] var currentPage: Seq[T] = Nil
  private[this] var currentOffset: Int = 0
  private[this] var currentIndex: Int = 0

  prepareNextResult()

  /**
    * Returns the next page of results starting at the specified
    * offset
    */
  def page(offset: Long): Iterable[T] = ???

  def hasNext: Boolean = !nextResult.isEmpty

  def next: T = {
    val result = nextResult.getOrElse {
      throw new NoSuchElementException()
    }
    prepareNextResult()
    result
  }

  private[this] def prepareNextResult() {
    currentPage.lift(currentIndex) match {
      case Some(result) => {
        this.nextResult = Some(result)
        currentIndex += 1
      }
      case None => {
        currentPage = page(currentOffset).toSeq
        currentOffset += currentPage.size
        this.nextResult = currentPage.headOption
        currentIndex = 1
      }
    }
  }

}

/**
  * Trait that enables us to iterate over a large number of results
  * (e.g. from a database query) one page at a time while deleting the results
  */
trait DeletionPager[T] extends Iterator[T] with Pager[T] {

  /**
    * Returns the next page of results starting at the specified
    */
  def page: Iterable[T]

}

package io.flow.play.postgresql

object Pager {

  /**
    * Helper to create a pager, inferring the types from the function
    * returning each page of results.
    * 
    * Example:
    * 
    *    Pager.create { offset =>
    *      SubscriptionsDao.findAll(
    *        publication = Some(Publication.DailySummary),
    *        offset = offset
    *      )
    *    }.map { subscription =>
    *      println(s"subscription: $subscription")
    *    }
    */
  def create[T](
    f: Long => Iterable[T]
  ): Pager[T] = {
    new Pager[T] {
      override def page(offset: Long): Iterable[T] = f(offset)
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
  def page(offset: Long): Iterable[T]

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

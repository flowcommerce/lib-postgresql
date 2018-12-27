package io.flow.postgresql

object Pager {

  /**
    * Helper to create a pager with an offset, inferring the types from the function returning each page of results.
    *
    * Example:
    *
    *    Pager.create { offset =>
    *      SubscriptionsDao.findAll(
    *        publication = Some(Publication.DailySummary),
    *        offset = offset
    *      )
    *    }.foreach { subscription =>
    *      println("subscription: " + subscription)
    *    }
    */
  def create[T](pagingFunction: Long => Iterable[T]): Pager[T] = byOffset[T](pagingFunction)

  /**
    * Helper to create a pager with an offset, inferring the types from the function returning each page of results.
    *
    * Example:
    *
    *    Pager.create { offset =>
    *      SubscriptionsDao.findAll(
    *        publication = Some(Publication.DailySummary),
    *        offset = offset
    *      )
    *    }.foreach { subscription =>
    *      println("subscription: " + subscription)
    *    }
    */
  def byOffset[T](pagingFunction: Long => Iterable[T]): Pager[T] = new Pager[T] {
    override def page(offset: Long): Iterable[T] = pagingFunction(offset)
  }

  /**
    * Helper to create a pager using on the last seen id.
    * This pager is useful when scanning large tables where using the offset may incur a performance hit
    *
    * Example:
    *
    *    Pager.byLastId[T, Id](
    *      { lastId: Option[Id] =>
    *        SubscriptionsDao.findAllWhereIdGreaterThan(
    *          publication = Some(Publication.DailySummary),
    *          minId = lastId
    *        )
    *      },
    *      subscription: T => subscription.id
    *    ).foreach { subscription =>
    *      println("subscription: " + subscription)
    *    }
    */
  def byLastId[T, Id](pagingFunction: Option[Id] => Iterable[T], getId: T => Id): LastIdPager[T, Id] = {
    new LastIdPager[T, Id] {
      override def page(lastId: Option[Id]): Iterable[T] = pagingFunction(lastId)
      override def extractId(element: T): Id = getId(element)
    }
  }

}

trait Pager[T] extends OffsetPager[T]

trait GenericPager[T, Context] extends Iterator[T] {

  private[this] var currentPageIterator: Iterator[T] = Iterator.empty
  private[this] var currentContext: Context = initialContext

  // load the first page
  loadPage()

  /**
    * Returns the initial context
    */
  def initialContext: Context

  /**
    * Updates the context keeping track of the traversal of the elements based on the next element produced.
    * This function is called every time the iterator produces the next element.
    * @param element the next element produced
    */
  def updateContext(currentContext: Context, element: T): Context

  /**
    * Produces the next page of elements
    * @param context the context keeping track of the traversal the elements
    * @return the next page of elements
    */
  def page(context: Context): Iterable[T]

  /**
    * Tests whether this iterator can provide another element.
    * @return `true` if a subsequent call to `next` will yield an element, `false` otherwise.
    */
  def hasNext: Boolean = currentPageIterator.hasNext

  /**
    * Produces the next element of this iterator.
    * @return the next element of this iterator, if `hasNext` is `true`, throws a [[scala.NoSuchElementException]] otherwise.
    */
  def next: T = {
    if (hasNext) nextAndReload()
    else throw new NoSuchElementException()
  }

  private def nextAndReload(): T = {
    val next = currentPageIterator.next
    currentContext = updateContext(currentContext, next)
    // end of the current page: load the next one
    if (!currentPageIterator.hasNext) loadPage()
    next
  }

  private def loadPage(): Unit = currentPageIterator = page(currentContext).iterator

}

trait OffsetPager[T] extends GenericPager[T, Long] {
  override def initialContext: Long = 0L
  override def updateContext(offset: Long, nextElement: T): Long = offset + 1
}

trait LastIdPager[T, Id] extends GenericPager[T, Option[Id]] {
  override def initialContext: Option[Id] = None
  override def updateContext(context: Option[Id], nextElement: T): Option[Id] = Some(extractId(nextElement))

  /**
    * Extracts the id of the element.
    * This function is called when the next element is produced to extract its id
    */
  def extractId(element: T): Id

}

package kyo.compat

/**
  * Uses a function that returns a fresh `Iterator[A]`, so a stream can run more than once. Elements are produced lazily on the thread that
  * consumes the iterator, and effectful stages run their `CIO` when the element is pulled. A failure in a stage is thrown from the
  * iterator's `hasNext` or `next`.
  */
opaque type CStream[+A] = () => Iterator[A]

object CStream {

  /**
    * Returns an empty stream.
    */
  def empty[A]: CStream[A] = () => Iterator.empty

  /**
    * Returns a stream of the elements produced by `c`.
    */
  def init[A](c: CIO[Seq[A]]): CStream[A] = () => c.lower.iterator

  /**
    * Returns a stream of the elements of `seq`.
    */
  def init[A](seq: Seq[A]): CStream[A] = () => seq.iterator

  /**
    * Returns the integers from `start` until `end`, excluding `end`.
    */
  def range(start: Int, end: Int): CStream[Int] = () => Iterator.range(start, end)

  /**
    * Unfolds a stream from `acc`. The stream ends when `f` returns `None`.
    */
  def unfold[S, A](acc: S)(f: S => CIO[Option[(A, S)]]): CStream[A] = () => Iterator.unfold(acc)(s => f(s).lower)

  /**
    * Wraps an iterator factory as a `CStream`. The factory runs each time the stream runs.
    */
  def lift[A](native: => Iterator[A]): CStream[A] = () => native

  extension [A](self: CStream[A]) {

    /**
      * Returns a new iterator over the stream. Each call runs the stream again.
      */
    def lower: Iterator[A] = self()

    /**
      * Appends `other` after this stream.
      */
    def concat[A2 >: A](other: CStream[A2]): CStream[A2] = () => self() ++ other()

    /**
      * Transforms each element with a pure function.
      */
    def mapPure[B](f: A => B): CStream[B] = () => self().map(f)

    /**
      * Transforms each element with an effectful function.
      */
    def map[B](f: A => CIO[B]): CStream[B] = () => self().map(a => f(a).lower)

    /**
      * Replaces each element with a stream and concatenates the results.
      */
    def flatMap[B](f: A => CStream[B]): CStream[B] = () => self().flatMap(a => f(a)())

    /**
      * Runs `f` for each element and passes the element through unchanged.
      */
    def tap(f: A => CIO[Any]): CStream[A] =
      () =>
        self().map { a =>
          val _ = f(a).lower
          a
        }

    /**
      * Keeps the first `n` elements.
      */
    def take(n: Int): CStream[A] = () => self().take(n)

    /**
      * Skips the first `n` elements.
      */
    def drop(n: Int): CStream[A] = () => self().drop(n)

    /**
      * Keeps elements while `p` holds.
      */
    def takeWhilePure(p: A => Boolean): CStream[A] = () => self().takeWhile(p)

    /**
      * Keeps the elements that satisfy the pure predicate.
      */
    def filterPure(p: A => Boolean): CStream[A] = () => self().filter(p)

    /**
      * Keeps the elements that satisfy the effectful predicate.
      */
    def filter(p: A => CIO[Boolean]): CStream[A] = () => self().filter(a => p(a).lower)

    /**
      * Transforms and filters the elements in one pass.
      */
    def collectPure[B](f: A => Option[B]): CStream[B] = () => self().flatMap(f)

    /**
      * Runs the stream and collects its elements.
      */
    def run: CIO[CChunk[A]] = CIO.defer(CChunk.lift(self().toVector))

    /**
      * Runs the stream and folds its elements with a pure function.
      */
    def foldPure[B](acc: B)(f: (B, A) => B): CIO[B] = CIO.defer(self().foldLeft(acc)(f))

    /**
      * Runs `f` for each element.
      */
    def foreach(f: A => CIO[Unit]): CIO[Unit] = CIO.defer(self().foreach(a => f(a).lower))

    /**
      * Runs the stream and discards its elements.
      */
    def discard: CIO[Unit] = CIO.defer(self().foreach(_ => ()))

  }

}

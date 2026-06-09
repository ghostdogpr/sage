package sage.client.internal

/**
  * The effect-typed seam behind a backend's native subscription stream: `next` yields the next message or `None` at end, `close`
  * unsubscribes. A backend wraps this with its native stream's finalizer so closing the stream's scope calls `close`.
  */
trait Subscription[F[_], A] {

  def next: F[Option[A]]

  def close: F[Unit]
}

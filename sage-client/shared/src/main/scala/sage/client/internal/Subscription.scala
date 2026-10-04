package sage.client.internal

/**
  * The shared subscription API used by backend-specific streams. `next` returns the next message or `None` when the subscription ends. It
  * fails with the server's error when the server refuses a subscribed name, including when a reconnect subscribes it again. `close`
  * unsubscribes. Each backend calls `close` from its stream finalizer when the stream's scope closes.
  */
trait Subscription[F[_], A] {

  def next: F[Option[A]]

  def close: F[Unit]
}

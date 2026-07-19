package sage.client.internal

import sage.Bytes

/**
  * The socket layer's boundary: the layer above speaks frames and send items, never bytes or threads. Implementations deliver every parsed
  * frame to an `onFrame` callback taken at construction, and invoke `onClosed` exactly once when the connection terminates for any reason,
  * including `close()`; by then every queued-but-unwritten item has been `dropped()`.
  */
private[client] trait Transport {

  /**
    * Begins the I/O. Called once, after the owner is ready to receive callbacks.
    */
  def start(): Unit

  /**
    * Enqueues an item for writing; never blocks. Exactly one of `writeAttempted`/`dropped` fires; `clearPayload` follows the capture.
    */
  def send(item: Transport.Item): Unit

  /**
    * Idempotent. Blocks until the I/O threads have terminated and `onClosed` has run.
    */
  def close(): Unit
}

private[client] object Transport {

  trait Item {

    def payload: Bytes

    /**
      * Invoked after the payload has been captured for the write attempt; it is never read again, so the item may drop it.
      */
    def clearPayload(): Unit = ()

    /**
      * Invoked on the writer thread immediately before the first write attempt: from here on the command may execute server-side.
      */
    def writeAttempted(): Unit

    /**
      * Invoked when the connection terminated before any write attempt.
      */
    def dropped(): Unit
  }
}

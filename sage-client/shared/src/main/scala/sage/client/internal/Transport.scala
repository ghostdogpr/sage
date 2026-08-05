package sage.client.internal

import sage.Bytes

/**
  * Defines the interface between connection logic and socket I/O. The connection submits [[Transport.Item]] values and receives parsed frames
  * through the `onFrame` callback supplied when the transport is created. The transport calls `onClosed` once when the connection ends,
  * including after `close()`. Before that callback, it calls `dropped()` for each queued item that was not written.
  */
private[client] trait Transport {

  /**
    * Begins the I/O. Called once, after the owner is ready to receive callbacks.
    */
  def start(): Unit

  /**
    * Adds an item to the write queue and returns immediately. The transport calls exactly one of `writeAttempted` or `dropped`. On the write
    * path, it calls `clearPayload` after capturing the bytes to write.
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
      * Invoked after the payload has been captured for writing. The item can release its reference to the payload at this point.
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

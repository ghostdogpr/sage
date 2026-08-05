package sage

/**
  * A user-supplied observer of runtime [[SageEvent]]s, registered in configuration. Listeners do not depend on a particular effect system or
  * backend. Sage dispatches events separately from command execution. A slow listener delays event delivery and may fill the dispatch queue;
  * exceptions thrown by a listener are ignored. Neither affects commands.
  */
trait SageListener {

  /**
    * Called once per runtime [[SageEvent]], synchronously and separately from command execution. The callback must not block. Exceptions are ignored.
    */
  def onEvent(event: SageEvent): Unit
}

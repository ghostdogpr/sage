package sage

/**
  * A user-supplied observer of runtime [[SageEvent]]s, registered in configuration. The callback is synchronous and `Unit`-returning so the
  * SPI is effect-agnostic and lives in the Core, letting an integration module bind to it without depending on any Backend. sage invokes it
  * off the command path, so a slow or throwing listener can never block or break command execution: a slow one only delays event delivery (and
  * sheds events once the dispatch queue fills), a throwing one is swallowed.
  */
trait SageListener {
  def onEvent(event: SageEvent): Unit
}

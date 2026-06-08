package sage.client.internal

import sage.protocol.Frame

/**
  * A `-READONLY` reply means the server was demoted from master without dropping the socket, so the connection looks healthy but rejects
  * writes; it must be discarded and reconnected rather than reused. Scoped to `READONLY` only — `LOADING` resolves on its own and the
  * cluster codes are a separate concern (ADR-0014).
  */
private[internal] object Poison {

  def isReadonly(frame: Frame): Boolean =
    frame match {
      case Frame.SimpleError(message) => errorCode(message) == "READONLY"
      case Frame.BulkError(message)   => errorCode(message.asUtf8String) == "READONLY"
      case _                          => false
    }

  private def errorCode(message: String): String = {
    val space = message.indexOf(' ')
    if (space < 0) message else message.substring(0, space)
  }
}

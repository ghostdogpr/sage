package sage.client.internal

import sage.protocol.Frame

/**
  * A `-READONLY` reply means that the server became a replica without dropping the socket. The connection still looks healthy but rejects
  * writes, so the client must replace it. Only `READONLY` poisons a connection here. `LOADING` resolves without a reconnect, and cluster
  * reply codes have separate handling.
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

package sage.client.internal

import sage.Bytes
import sage.protocol.Frame

/**
  * Transport factories over a [[FakeTransport]]; `capturing` also hands back the most-recently-created one.
  */
object ScriptedTransport {

  def factory(respond: Bytes => Seq[Frame]): MultiplexedConnection.TransportFactory =
    (onFrame, onClosed) => new FakeTransport(onFrame, onClosed, respond)

  def capturing(factory: MultiplexedConnection.TransportFactory): (MultiplexedConnection.TransportFactory, () => FakeTransport) = {
    var last: FakeTransport                             = null
    val wrapped: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      last = factory(onFrame, onClosed).asInstanceOf[FakeTransport]
      last
    }
    (wrapped, () => last)
  }

  def apply(respond: Bytes => Seq[Frame]): (MultiplexedConnection.TransportFactory, () => FakeTransport) = capturing(factory(respond))
}

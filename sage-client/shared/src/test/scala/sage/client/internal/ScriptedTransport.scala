package sage.client.internal

import java.util.concurrent.atomic.AtomicReference

import sage.Bytes
import sage.protocol.Frame

/**
  * Creates transport factories backed by [[FakeTransport]]. `capturing` also returns a function for reading the most recently created transport.
  */
object ScriptedTransport {

  type Factory = (Frame => Unit, () => Unit) => FakeTransport

  def factory(respond: Bytes => Seq[Frame]): Factory =
    (onFrame, onClosed) => new FakeTransport(onFrame, onClosed, respond)

  def capturing(factory: Factory): (Factory, () => FakeTransport) = {
    val created          = new AtomicReference[FakeTransport]()
    val wrapped: Factory = (onFrame, onClosed) => {
      val transport = factory(onFrame, onClosed)
      created.set(transport)
      transport
    }
    def last()           =
      created.get() match {
        case null      => throw new IllegalStateException("no transport has been created yet")
        case transport => transport
      }
    (wrapped, last)
  }

  def apply(respond: Bytes => Seq[Frame]): (Factory, () => FakeTransport) = capturing(factory(respond))
}

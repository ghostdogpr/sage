package sage.client.internal

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import sage.{SageEvent, SageListener}

final private[sage] class ConnectFailureRecorder {

  private val delivered = new CountDownLatch(1)
  private val seen      = new ConcurrentLinkedQueue[SageEvent.Connection.ConnectFailed]()

  val events: Events = Events(
    Vector(
      new SageListener {
        def onEvent(event: SageEvent): Unit = event match {
          case failure: SageEvent.Connection.ConnectFailed =>
            seen.add(failure)
            delivered.countDown()
          case _                                           => ()
        }
      }
    )
  )

  def await(timeout: FiniteDuration = 2.seconds): Boolean = delivered.await(timeout.toNanos, TimeUnit.NANOSECONDS)

  def failures: Vector[SageEvent.Connection.ConnectFailed] = seen.asScala.toVector
}

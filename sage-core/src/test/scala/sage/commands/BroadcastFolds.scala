package sage.commands

import sage.protocol.Frame

/**
  * Exposes the reducer stored by a broadcast command so tests can combine multi-master replies without a cluster.
  */
trait BroadcastFolds { this: munit.FunSuite =>

  protected def fold(command: Command[?]): (Frame, Frame) => Frame =
    command.broadcast match {
      case BroadcastReduce.Fold(combine) => combine
      case other                         => fail(s"expected a Fold broadcast, got $other")
    }
}

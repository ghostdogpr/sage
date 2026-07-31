package sage.commands

import sage.protocol.Frame

/**
  * Reaches the reducer a broadcast command carries, so a spec can fold multi-master replies without a cluster.
  */
trait BroadcastFolds { this: munit.FunSuite =>

  protected def fold(command: Command[?]): (Frame, Frame) => Frame =
    command.broadcast match {
      case BroadcastReduce.Fold(combine) => combine
      case other                         => fail(s"expected a Fold broadcast, got $other")
    }
}

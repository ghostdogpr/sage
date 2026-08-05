package sage.commands

import scala.collection.mutable

import sage.protocol.Frame

/**
  * Pairwise reply combiners used by [[BroadcastReduce.Fold]]. If either reply has an unexpected shape, the combiner returns that reply for
  * the command's decoder to reject. Command-specific validation remains with the command. For example, `SCRIPT EXISTS` must also verify
  * that both arrays describe the same SHAs, which a general array combiner cannot do.
  */
private[commands] object Merge {

  val sum: (Frame, Frame) => Frame = integers((x, y) => math.addExact(x, y))

  val min: (Frame, Frame) => Frame = integers((x, y) => math.min(x, y))

  /**
    * Appends two arrays, dropping repeats: a classic channel can hold subscribers on several masters, so more than one reports it.
    */
  val distinctChannels: (Frame, Frame) => Frame = (a, b) =>
    (a, b) match {
      case (Frame.Array(x), Frame.Array(y)) => Frame.Array((x ++ y).distinct)
      case (Frame.Array(_), bad)            => bad
      case (bad, _)                         => bad
    }

  /**
    * Sums a flat `[channel, count, …]` reply by channel and preserves the order in which channels first appear. Simply appending replies
    * would repeat channels, and converting the result to a `Map` would discard all but the last count for each one.
    */
  val sumByChannel: (Frame, Frame) => Frame = (a, b) =>
    (channelCounts(a), channelCounts(b)) match {
      case (Some(x), Some(y)) =>
        val totals = mutable.LinkedHashMap.empty[Frame.BulkString, Long]
        (x ++ y).foreach { case (channel, count) => totals.update(channel, math.addExact(totals.getOrElse(channel, 0L), count)) }
        Frame.Array(totals.iterator.flatMap { case (channel, total) => Vector(channel, Frame.Integer(total)) }.toVector)
      case (None, _)          => a
      case _                  => b
    }

  /**
    * Extracts channel/count pairs from a flat `[channel, count, …]` reply. Returns `None` for any other shape. The merge and the `PUBSUB
    * NUMSUB` decoder share this function so they apply the same validation.
    */
  def channelCounts(frame: Frame): Option[Vector[(Frame.BulkString, Long)]] =
    frame match {
      case Frame.Array(elements) if elements.length % 2 == 0 =>
        val builder = Vector.newBuilder[(Frame.BulkString, Long)]
        var i       = 0
        while (i < elements.length) {
          (elements(i), elements(i + 1)) match {
            case (channel: Frame.BulkString, Frame.Integer(count)) => builder += ((channel, count))
            case _                                                 => return None
          }
          i += 2
        }
        Some(builder.result())
      case _ => None
    }

  private def integers(combine: (Long, Long) => Long): (Frame, Frame) => Frame = (a, b) =>
    (a, b) match {
      case (Frame.Integer(x), Frame.Integer(y)) => Frame.Integer(combine(x, y))
      case (Frame.Integer(_), bad)              => bad
      case (bad, _)                             => bad
    }
}

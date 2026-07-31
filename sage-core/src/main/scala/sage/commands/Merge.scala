package sage.commands

import scala.collection.mutable

import sage.protocol.Frame

/**
  * Shape-generic pairwise combiners for [[BroadcastReduce.Fold]], folding two masters' replies into one. Each passes a reply that does not
  * fit the expected shape straight through, in either operand position, so the command's own decoder reports it rather than this merge
  * hiding it behind a well-formed sibling. A combiner that needs command-specific validation stays with its command instead, since it can
  * reject what no downstream decoder would catch (`SCRIPT EXISTS` compares arrays that must describe the same SHAs).
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
    * Sums a flat `[channel, count, …]` reply per channel, keeping each channel's first-seen position. Appending instead would emit a channel
    * once per master, and the `PUBSUB NUMSUB` decoder builds a `Map`, so every count but the last would be dropped.
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
    * The channel/count pairs of a flat `[channel, count, …]` reply, or `None` when it is not that shape. Shared with the `PUBSUB NUMSUB`
    * decoder so the merge and the decode cannot disagree about the reply's shape.
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

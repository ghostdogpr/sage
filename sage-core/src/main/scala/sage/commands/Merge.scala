package sage.commands

import sage.SageException.DecodeError
import sage.protocol.Frame

/**
  * Pairwise reply combiners used by [[BroadcastReduce.Fold]]. If either reply has an unexpected shape, the combiner throws a
  * `DecodeError`. Command-specific validation remains with the command. For example, `SCRIPT EXISTS` must also verify
  * that both arrays describe the same SHAs, which a general array combiner cannot do.
  */
private[commands] object Merge {

  val sum: (Frame, Frame) => Frame = typed(Decode.long, Frame.Integer(_))((x, y) => math.addExact(x, y))

  val min: (Frame, Frame) => Frame = typed(Decode.long, Frame.Integer(_))((x, y) => math.min(x, y))

  // appends two arrays, dropping repeats: a classic channel can hold subscribers on several masters, so more than one reports it
  val distinct: (Frame, Frame) => Frame = typed(Decode.vector(Decode.frame), Frame.Array(_))((x, y) => (x ++ y).distinct)

  // decoding both replies with the command's decoder makes the merge accept exactly what the decoder accepts
  def typed[A](decode: Frame => Either[DecodeError, A], encode: A => Frame)(combine: (A, A) => A): (Frame, Frame) => Frame = (a, b) =>
    decode(a).flatMap(x => decode(b).map(y => encode(combine(x, y)))).fold(error => throw error, identity)
}

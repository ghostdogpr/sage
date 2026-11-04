package sage.client.inmemory

import sage.Bytes
import sage.protocol.Frame

/**
  * `LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]`, the longest common subsequence of two strings. The `IDX` matches are
  * found by walking the dynamic-programming table backwards from the end of both strings, which is how Redis finds them, so they are
  * listed from the end of the strings towards the start.
  */
private[inmemory] object Lcs {
  import Replies.*

  def run(call: Call): Frame = {
    var len          = false
    var idx          = false
    var minMatchLen  = 0L
    var withMatchLen = false
    var i            = 2
    while (i < call.args.length) {
      call.keyword(i) match {
        case "LEN"                                     => len = true
        case "IDX"                                     => idx = true
        case "WITHMATCHLEN"                            => withMatchLen = true
        case "MINMATCHLEN" if i + 1 < call.args.length =>
          i += 1
          minMatchLen = math.max(0L, call.long(i))
        case _                                         => throw new Reject(Errors.syntax)
      }
      i += 1
    }
    if (len && idx) throw new Reject(Errors.err("If you want both the length and indexes, please just use IDX."))
    val a            = call.string(call.key(0)).getOrElse(Bytes.empty).toIArray
    val b            = call.string(call.key(1)).getOrElse(Bytes.empty).toIArray
    // table(i)(j) is the LCS length of the first i bytes of a and the first j bytes of b
    val table        = Array.ofDim[Int](a.length + 1, b.length + 1)
    for {
      x <- 1 to a.length
      y <- 1 to b.length
    }
      table(x)(y) = if (a(x - 1) == b(y - 1)) table(x - 1)(y - 1) + 1 else math.max(table(x - 1)(y), table(x)(y - 1))
    val total        = table(a.length)(b.length)
    if (len) int(total.toLong)
    else {
      val common  = new Array[Byte](total)
      var matches = Vector.empty[Frame]
      var x       = a.length
      var y       = b.length
      var k       = total
      // the open range, inclusive; aStart == a.length means no range is open
      var aStart  = a.length
      var aEnd    = 0
      var bStart  = 0
      var bEnd    = 0
      while (x > 0 && y > 0) {
        var emit = false
        if (a(x - 1) == b(y - 1)) {
          common(k - 1) = a(x - 1)
          if (aStart == a.length) {
            aStart = x - 1; aEnd = x - 1; bStart = y - 1; bEnd = y - 1
          } else if (aStart == x && bStart == y) {
            aStart -= 1; bStart -= 1
          } else emit = true
          if (aStart == 0 || bStart == 0) emit = true
          k -= 1; x -= 1; y -= 1
        } else {
          if (table(x - 1)(y) > table(x)(y - 1)) x -= 1 else y -= 1
          if (aStart != a.length) emit = true
        }
        if (emit) {
          val matchLen = (aEnd - aStart + 1).toLong
          if (minMatchLen == 0 || matchLen >= minMatchLen) {
            val ranges = Vector(
              Frame.Array(Vector(int(aStart.toLong), int(aEnd.toLong))),
              Frame.Array(Vector(int(bStart.toLong), int(bEnd.toLong)))
            )
            matches :+= Frame.Array(if (withMatchLen) ranges :+ int(matchLen) else ranges)
          }
          aStart = a.length
        }
      }
      if (idx) Frame.Map(Vector(bulk("matches") -> Frame.Array(matches), bulk("len") -> int(total.toLong)))
      else bulk(Bytes.wrap(IArray.unsafeFromArray(common)))
    }
  }
}

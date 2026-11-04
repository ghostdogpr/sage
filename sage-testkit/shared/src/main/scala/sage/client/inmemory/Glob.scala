package sage.client.inmemory

import sage.Bytes

/**
  * Matches keys against the glob patterns of `KEYS` and `SCAN … MATCH`, byte by byte like Redis. `*` matches any sequence, `?` matches one
  * byte, `[abc]`, `[a-z]` and `[^a]` match a set of bytes, and `\` makes the next byte literal.
  */
private[inmemory] object Glob {

  def matches(pattern: Bytes, subject: Bytes): Boolean = matchFrom(pattern.toIArray, 0, subject.toIArray, 0)

  private def matchFrom(p: IArray[Byte], pi: Int, s: IArray[Byte], si: Int): Boolean =
    if (pi == p.length) si == s.length
    else
      p(pi).toChar match {
        case '*'                       =>
          // consecutive stars behave like one
          var next = pi
          while (next < p.length && p(next) == '*') next += 1
          if (next == p.length) true
          else (si to s.length).exists(start => matchFrom(p, next, s, start))
        case '?'                       => si < s.length && matchFrom(p, pi + 1, s, si + 1)
        case '['                       =>
          si < s.length && {
            val (matched, after) = matchClass(p, pi + 1, s(si))
            matched && matchFrom(p, after, s, si + 1)
          }
        case '\\' if pi + 1 < p.length =>
          si < s.length && p(pi + 1) == s(si) && matchFrom(p, pi + 2, s, si + 1)
        case literal                   =>
          si < s.length && literal == s(si).toChar && matchFrom(p, pi + 1, s, si + 1)
      }

  // Returns whether `byte` is in the class starting at `start` (just after `[`) and the pattern index after the closing `]`. An unclosed
  // class extends to the end of the pattern, as in Redis.
  private def matchClass(p: IArray[Byte], start: Int, byte: Byte): (Boolean, Int) = {
    var i       = start
    val negated = i < p.length && p(i) == '^'
    if (negated) i += 1
    var matched = false
    while (i < p.length && p(i) != ']')
      if (p(i) == '\\' && i + 1 < p.length) {
        if (p(i + 1) == byte) matched = true
        i += 2
      } else if (i + 2 < p.length && p(i + 1) == '-') {
        val low  = math.min(p(i) & 0xff, p(i + 2) & 0xff)
        val high = math.max(p(i) & 0xff, p(i + 2) & 0xff)
        if ((byte & 0xff) >= low && (byte & 0xff) <= high) matched = true
        i += 3
      } else {
        if (p(i) == byte) matched = true
        i += 1
      }
    val after   = if (i < p.length) i + 1 else i
    (matched != negated, after)
  }
}

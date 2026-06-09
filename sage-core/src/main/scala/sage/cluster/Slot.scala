package sage.cluster

import sage.Bytes

/**
  * One of the [[Slot.Count]] hash slots a cluster partitions the keyspace into, fixed by CRC16 of the key's hash tag (or the whole key).
  */
opaque type Slot = Int

object Slot {

  val Count = 16384

  /**
    * A slot by index, or `None` when out of range; total, for redirect parsing and untrusted input.
    */
  def at(index: Int): Option[Slot] = if (index >= 0 && index < Count) Some(index) else None

  /**
    * Wrap an index already known in range (a hash modulo, a validated bound). Unchecked: out-of-range breaks topology lookup.
    */
  private[sage] def unsafe(index: Int): Slot = index

  def of(key: Bytes): Slot = {
    val bytes = key.unsafeArray // read-only; CRC16 never mutates
    val open  = indexOf(bytes, '{'.toByte, 0)
    if (open < 0) crc(bytes, 0, bytes.length)
    else {
      val close = indexOf(bytes, '}'.toByte, open + 1)
      // a tag hashes only the run between the first '{' and the next '}', and only when non-empty; otherwise the whole key
      if (close < 0 || close == open + 1) crc(bytes, 0, bytes.length)
      else crc(bytes, open + 1, close)
    }
  }

  extension (self: Slot) {
    def value: Int = self
  }

  private def crc(bytes: Array[Byte], start: Int, end: Int): Slot = Crc16.of(bytes, start, end) % Count

  private def indexOf(bytes: Array[Byte], target: Byte, from: Int): Int = {
    var i = from
    while (i < bytes.length && bytes(i) != target) i += 1
    if (i < bytes.length) i else -1
  }
}

/**
  * CRC16-CCITT (XMODEM polynomial 0x1021, zero seed), the function Redis Cluster hashes keys with. The table is derived at load rather
  * than hard-coded so it is self-evidently the standard polynomial.
  */
private[cluster] object Crc16 {

  private val table: Array[Int] = {
    val t = new Array[Int](256)
    var i = 0
    while (i < 256) {
      var crc = i << 8
      var j   = 0
      while (j < 8) {
        crc = if ((crc & 0x8000) != 0) (crc << 1) ^ 0x1021 else crc << 1
        j += 1
      }
      t(i) = crc & 0xffff
      i += 1
    }
    t
  }

  def of(bytes: Array[Byte], start: Int, end: Int): Int = {
    var crc = 0
    var i   = start
    while (i < end) {
      crc = ((crc << 8) ^ table(((crc >>> 8) ^ (bytes(i) & 0xff)) & 0xff)) & 0xffff
      i += 1
    }
    crc
  }
}

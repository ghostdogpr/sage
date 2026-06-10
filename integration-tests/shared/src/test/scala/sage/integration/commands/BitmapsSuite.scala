package sage.integration.commands

import kyo.compat.*

import sage.commands.{BitFieldOffset, BitFieldOp, BitFieldOverflow, BitFieldType, BitRange, BitUnit}
import sage.integration.{Images, ServerSuite}

abstract class BitmapsSuite(image: String) extends ServerSuite(image) {

  test("SETBIT GETBIT and BITCOUNT track individual bits") {
    withClient { client =>
      for {
        prev  <- client.setBit("bits", 7L, true)
        bit7  <- client.getBit("bits", 7L)
        bit6  <- client.getBit("bits", 6L)
        count <- client.bitCount("bits")
      } yield {
        assertEquals(prev, false)
        assertEquals(bit7, true)
        assertEquals(bit6, false)
        assertEquals(count, 1L)
      }
    }
  }

  test("BITCOUNT and BITPOS honor byte and bit ranges") {
    withClient { client =>
      for {
        _         <- client.set("bitstr", "foobar")
        whole     <- client.bitCount("bitstr")
        byteRange <- client.bitCount("bitstr", Some(BitRange(1L, 1L)))
        bitRange  <- client.bitCount("bitstr", Some(BitRange(5L, 30L, BitUnit.Bit)))
        firstSet  <- client.bitPos("bitstr", true)
        firstZero <- client.bitPos("bitstr", false)
      } yield {
        assertEquals(whole, 26L)
        assertEquals(byteRange, 6L)
        assertEquals(bitRange, 17L)
        assertEquals(firstSet, 1L)
        assertEquals(firstZero, 0L)
      }
    }
  }

  test("BITOP combines bitmaps into a destination") {
    withClient { client =>
      for {
        _    <- client.set("opa", "abc")
        _    <- client.set("opb", "abd")
        andN <- client.bitOpAnd("opand", "opa", "opb")
        orN  <- client.bitOpOr("opor", "opa", "opb")
        xorN <- client.bitOpXor("opxor", "opa", "opb")
        notN <- client.bitOpNot("opnot", "opa")
      } yield {
        assertEquals(andN, 3L)
        assertEquals(orN, 3L)
        assertEquals(xorN, 3L)
        assertEquals(notN, 3L)
      }
    }
  }

  test("BITFIELD runs typed sub-operations with overflow control") {
    withClient { client =>
      for {
        results  <- client.bitField(
                      "bf",
                      BitFieldOp.Set(BitFieldType.Unsigned(8), BitFieldOffset.Absolute(0L), 255L),
                      BitFieldOp.Overflow(BitFieldOverflow.Fail),
                      BitFieldOp.IncrBy(BitFieldType.Unsigned(8), BitFieldOffset.Absolute(0L), 10L)
                    )
        readBack <- client.bitFieldRo("bf", BitFieldOp.Get(BitFieldType.Unsigned(8), BitFieldOffset.Absolute(0L)))
      } yield {
        assertEquals(results, Vector(Some(0L), None))
        assertEquals(readBack, Vector(255L))
      }
    }
  }
}

class RedisBitmapsSuite extends BitmapsSuite(Images.redis)

class ValkeyBitmapsSuite extends BitmapsSuite(Images.valkey)

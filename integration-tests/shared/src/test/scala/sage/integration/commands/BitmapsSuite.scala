package sage.integration.commands

import sage.commands.{BitFieldOffset, BitFieldOp, BitFieldOverflow, BitFieldType, BitRange, BitUnit}
import sage.integration.BothServersSuite

class BitmapsSuite extends BothServersSuite {

  clientTest("SETBIT GETBIT and BITCOUNT track individual bits") { client =>
    client.setBit("bits", 7L, true).is(false) >>
      client.getBit("bits", 7L).is(true) >>
      client.getBit("bits", 6L).is(false) >>
      client.bitCount("bits").is(1L)
  }

  clientTest("BITCOUNT and BITPOS honor byte and bit ranges") { client =>
    client.set("bitstr", "foobar") >>
      client.bitCount("bitstr").is(26L) >>
      client.bitCount("bitstr", Some(BitRange(1L, 1L))).is(6L) >>
      client.bitCount("bitstr", Some(BitRange(5L, 30L, BitUnit.Bit))).is(17L) >>
      client.bitPos("bitstr", true).is(1L) >>
      client.bitPos("bitstr", false).is(0L)
  }

  clientTest("BITOP combines bitmaps into a destination") { client =>
    client.set("opa", "abc") >>
      client.set("opb", "abd") >>
      client.bitOpAnd("opand", "opa", "opb").is(3L) >>
      client.bitOpOr("opor", "opa", "opb").is(3L) >>
      client.bitOpXor("opxor", "opa", "opb").is(3L) >>
      client.bitOpNot("opnot", "opa").is(3L)
  }

  clientTest("BITFIELD runs typed sub-operations with overflow control") { client =>
    client
      .bitField(
        "bf",
        BitFieldOp.Set(BitFieldType.Unsigned(8), BitFieldOffset.Absolute(0L), 255L),
        BitFieldOp.Overflow(BitFieldOverflow.Fail),
        BitFieldOp.IncrBy(BitFieldType.Unsigned(8), BitFieldOffset.Absolute(0L), 10L)
      )
      .is(Vector(Some(0L), None)) >>
      client.bitFieldRo("bf", BitFieldOp.Get(BitFieldType.Unsigned(8), BitFieldOffset.Absolute(0L))).is(Vector(255L))
  }
}

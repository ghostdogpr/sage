package sage.commands

import java.nio.charset.StandardCharsets

class CommandSamplesSpec extends munit.FunSuite {

  // independent of RespWriter on purpose: a shared bug would cancel out
  private def resp(words: Vector[String]): String =
    words.map(w => s"$$${w.getBytes(StandardCharsets.UTF_8).length}\r\n$w\r\n").mkString(s"*${words.size}\r\n", "", "")

  CommandSamples.all.zipWithIndex.foreach { case (sample, index) =>
    test(s"sample $index (${sample.wire.mkString(" ")}) encodes against the golden wire frame") {
      assertEquals(sample.command.encode.asUtf8String, resp(sample.wire))
    }
  }

  test("every sample's wire starts with its command name") {
    CommandSamples.all.foreach { sample =>
      assertEquals(sample.wire.take(sample.command.name.split(' ').length), sample.command.name.split(' ').toVector)
    }
  }
}

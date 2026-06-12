package sage.examples.ox

import ox.Ox

import sage.*
import sage.ox.*

/**
  * Redis Streams: append entries, read them back by range, then consume them through a Consumer Group with explicit acknowledgement. The
  * stream is reset first so the example is deterministic on re-run.
  */
object StreamsExample {

  def run(client: SageClient)(using Ox): Unit = {
    val _   = client.del("stream:orders")
    val _   = client.xAdd("stream:orders")(("item", "book"), ("qty", "2"))
    val _   = client.xAdd("stream:orders")(("item", "pen"), ("qty", "5"))
    val len = client.xLen("stream:orders")

    val entries = client.xRange[String, String, String]("stream:orders")

    // a Consumer Group reading from the start of the stream
    val _       = client.xGroupCreate("stream:orders", "workers", id = GroupStartId.At(StreamId.Zero))
    val batches = client.xReadGroup[String, String, String]("workers", "w1")(("stream:orders", GroupReadId.New))()
    val ids     = batches.flatMap(_._2).map(_.id)
    val _       = client.xAck("stream:orders", "workers")(ids.head, ids.tail*)

    println(s"len=$len read=${entries.size} acked=${ids.size}")
  }
}

package sage.examples.pekko

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import sage.*
import sage.backend.*

/**
  * Redis Streams: append entries, read them back by range, then consume them through a Consumer Group with explicit acknowledgement. The
  * stream is reset first so the example is deterministic on re-run.
  */
object StreamsExample {

  def run(client: SageClient)(using ExecutionContext): Future[Unit] =
    for {
      _       <- client.del("stream:orders")
      _       <- client.xAdd("stream:orders")(("item", "book"), ("qty", "2"))
      _       <- client.xAdd("stream:orders")(("item", "pen"), ("qty", "5"))
      len     <- client.xLen("stream:orders")
      entries <- client.xRange[String, String]("stream:orders")
      // a Consumer Group reading from the start of the stream
      _       <- client.xGroupCreate("stream:orders", "workers", id = GroupStartId.At(StreamId.Zero))
      batches <- client.xReadGroup[String, String]("workers", "w1")(("stream:orders", GroupReadId.New))()
      ids      = batches.flatMap(_._2).map(_.id)
      _       <- client.xAck("stream:orders", "workers")(ids.head, ids.tail*)
    } yield println(s"len=$len read=${entries.size} acked=${ids.size}")
}

package sage.commands

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.Args.Count
import sage.protocol.Frame

/**
  * `XINFO STREAM`: the summary view. Version-specific fields (7.0+) are `Option`, decoded leniently on presence.
  */
final case class StreamInfo[F, V](
  length: Long,
  radixTreeKeys: Long,
  radixTreeNodes: Long,
  lastGeneratedId: StreamId,
  maxDeletedEntryId: Option[StreamId],
  entriesAdded: Option[Long],
  recordedFirstEntryId: Option[StreamId],
  groups: Long,
  firstEntry: Option[StreamEntry[F, V]],
  lastEntry: Option[StreamEntry[F, V]]
)

/**
  * One row of `XINFO GROUPS`. `entriesRead`/`lag` exist from 7.0 (and `lag` may be null even then), so both are `Option`.
  */
final case class GroupInfo(
  name: String,
  consumers: Long,
  pending: Long,
  lastDeliveredId: StreamId,
  entriesRead: Option[Long],
  lag: Option[Long]
)

/**
  * One row of `XINFO CONSUMERS`. `inactive` exists from 7.2.
  */
final case class ConsumerInfo(name: String, pending: Long, idle: FiniteDuration, inactive: Option[FiniteDuration])

/**
  * A row of a full PEL listing: the entry id, the instant it was last delivered, and how many times it has been delivered. `consumer` is
  * the owning consumer at the group level (empty string for an `XNACK`-released entry) and `None` at the consumer level, where it is implied.
  */
final case class FullPendingEntry(id: StreamId, consumer: Option[String], lastDelivered: Instant, deliveryCount: Long)

/**
  * A consumer inside `XINFO STREAM ... FULL`, with its own slice of the PEL.
  */
final case class FullConsumerInfo(
  name: String,
  seenTime: Option[Instant],
  activeTime: Option[Instant],
  pelCount: Option[Long],
  pending: Vector[FullPendingEntry]
)

/**
  * A group inside `XINFO STREAM ... FULL`, with the group-level PEL and its consumers.
  */
final case class FullGroupInfo(
  name: String,
  lastDeliveredId: StreamId,
  pelCount: Option[Long],
  entriesRead: Option[Long],
  lag: Option[Long],
  pending: Vector[FullPendingEntry],
  consumers: Vector[FullConsumerInfo]
)

/**
  * `XINFO STREAM ... FULL`: the deep view, inlining every entry, group, PEL and consumer. Decoded leniently on presence.
  */
final case class StreamInfoFull[F, V](
  length: Long,
  radixTreeKeys: Long,
  radixTreeNodes: Long,
  lastGeneratedId: StreamId,
  maxDeletedEntryId: Option[StreamId],
  entriesAdded: Option[Long],
  recordedFirstEntryId: Option[StreamId],
  entries: Vector[StreamEntry[F, V]],
  groups: Vector[FullGroupInfo]
)

private[sage] object StreamInfo {

  def xInfoStream[K, F, V](key: K)(using keyCodec: KeyCodec[K], fieldCodec: KeyCodec[F], valueCodec: ValueCodec[V]): Command[StreamInfo[F, V]] =
    Command.readUncacheable("XINFO STREAM", Command.FirstKey, Vector(keyCodec.encode(key)), streamReply[F, V])

  def xInfoStreamFull[K, F, V](key: K, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    fieldCodec: KeyCodec[F],
    valueCodec: ValueCodec[V]
  ): Command[StreamInfoFull[F, V]] =
    Command.readUncacheable(
      "XINFO STREAM",
      Command.FirstKey,
      Vector(keyCodec.encode(key), Full) ++ Args.optLong(Count, count),
      streamFullReply[F, V]
    )

  def xInfoGroups[K](key: K)(using keyCodec: KeyCodec[K]): Command[Vector[GroupInfo]] =
    Command.readUncacheable("XINFO GROUPS", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.vector(groupReply))

  def xInfoConsumers[K](key: K, group: String)(using keyCodec: KeyCodec[K]): Command[Vector[ConsumerInfo]] =
    Command.readUncacheable("XINFO CONSUMERS", Command.FirstKey, Vector(keyCodec.encode(key), sage.Bytes.utf8(group)), Decode.vector(consumerReply))

  private def streamReply[F, V](using KeyCodec[F], ValueCodec[V]): Frame => Either[DecodeError, StreamInfo[F, V]] =
    Decode.fields { f =>
      for {
        length   <- f.required("length", Decode.long)
        rtKeys   <- f.requiredOr("radix-tree-keys", Decode.long, 0L)
        rtNodes  <- f.requiredOr("radix-tree-nodes", Decode.long, 0L)
        lastId   <- f.required("last-generated-id", Streams.streamId)
        maxDel   <- f.optional("max-deleted-entry-id", Streams.streamId)
        added    <- f.optional("entries-added", Decode.long)
        firstRec <- f.optional("recorded-first-entry-id", Streams.streamId)
        groups   <- f.requiredOr("groups", Decode.long, 0L)
        firstE   <- f.optional("first-entry", Streams.streamEntry[F, V])
        lastE    <- f.optional("last-entry", Streams.streamEntry[F, V])
      } yield StreamInfo(length, rtKeys, rtNodes, lastId, maxDel, added, firstRec, groups, firstE, lastE)
    }

  private def streamFullReply[F, V](using KeyCodec[F], ValueCodec[V]): Frame => Either[DecodeError, StreamInfoFull[F, V]] =
    Decode.fields { f =>
      for {
        length   <- f.required("length", Decode.long)
        rtKeys   <- f.requiredOr("radix-tree-keys", Decode.long, 0L)
        rtNodes  <- f.requiredOr("radix-tree-nodes", Decode.long, 0L)
        lastId   <- f.required("last-generated-id", Streams.streamId)
        maxDel   <- f.optional("max-deleted-entry-id", Streams.streamId)
        added    <- f.optional("entries-added", Decode.long)
        firstRec <- f.optional("recorded-first-entry-id", Streams.streamId)
        entries  <- f.optionalVector("entries", Streams.streamEntry[F, V])
        groups   <- f.optionalVector("groups", fullGroupReply)
      } yield StreamInfoFull(length, rtKeys, rtNodes, lastId, maxDel, added, firstRec, entries, groups)
    }

  private val groupReply: Frame => Either[DecodeError, GroupInfo] =
    Decode.fields { f =>
      for {
        name      <- f.required("name", Decode.utf8String)
        consumers <- f.requiredOr("consumers", Decode.long, 0L)
        pending   <- f.requiredOr("pending", Decode.long, 0L)
        lastId    <- f.required("last-delivered-id", Streams.streamId)
        read      <- f.optional("entries-read", Decode.long)
        lag       <- f.optional("lag", Decode.long)
      } yield GroupInfo(name, consumers, pending, lastId, read, lag)
    }

  private val consumerReply: Frame => Either[DecodeError, ConsumerInfo] =
    Decode.fields { f =>
      for {
        name     <- f.required("name", Decode.utf8String)
        pending  <- f.requiredOr("pending", Decode.long, 0L)
        idle     <- f.required("idle", Decode.millisDuration)
        inactive <- f.optional("inactive", Decode.millisDuration)
      } yield ConsumerInfo(name, pending, idle, inactive)
    }

  private val fullGroupReply: Frame => Either[DecodeError, FullGroupInfo] =
    Decode.fields { f =>
      for {
        name      <- f.required("name", Decode.utf8String)
        lastId    <- f.required("last-delivered-id", Streams.streamId)
        pelCount  <- f.optional("pel-count", Decode.long)
        read      <- f.optional("entries-read", Decode.long)
        lag       <- f.optional("lag", Decode.long)
        pending   <- f.optionalVector("pending", groupPendingReply)
        consumers <- f.optionalVector("consumers", fullConsumerReply)
      } yield FullGroupInfo(name, lastId, pelCount, read, lag, pending, consumers)
    }

  private val fullConsumerReply: Frame => Either[DecodeError, FullConsumerInfo] =
    Decode.fields { f =>
      for {
        name    <- f.required("name", Decode.utf8String)
        seen    <- f.optional("seen-time", Decode.millisInstant)
        active  <- f.optional("active-time", Decode.millisInstant)
        pel     <- f.optional("pel-count", Decode.long)
        pending <- f.optionalVector("pending", consumerPendingReply)
      } yield FullConsumerInfo(name, seen, active, pel, pending)
    }

  // A group-level FULL PEL row includes its owning consumer: [id, consumer, delivery-time-ms, delivery-count].
  private val groupPendingReply: Frame => Either[DecodeError, FullPendingEntry] =
    Decode.array4(
      Streams.streamId,
      Decode.utf8String,
      Decode.millisInstant,
      Decode.long,
      "group pending [id, consumer, delivery-time, delivery-count]"
    ) { (id, consumer, delivered, count) =>
      FullPendingEntry(id, Some(consumer), delivered, count)
    }

  // a consumer-level FULL PEL row omits the (implied) consumer: [id, delivery-time-ms, delivery-count]
  private val consumerPendingReply: Frame => Either[DecodeError, FullPendingEntry] =
    Decode.array3(Streams.streamId, Decode.millisInstant, Decode.long, "consumer pending [id, delivery-time, delivery-count]") {
      (id, delivered, count) =>
        FullPendingEntry(id, None, delivered, count)
    }

  private val Full = sage.Bytes.utf8("FULL")
}

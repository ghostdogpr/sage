package sage.zio

import scala.concurrent.duration.FiniteDuration

import kyo.compat.*
import zio.*
import zio.stream.ZStream

import sage.{Message, PatternMessage}
import sage.client.SageConfig
import sage.client.internal.{Client, Subscription, TransactionScope}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Command, Hashes, Keys, Pipeline, RedisType, ScanCursor, ScanPage, Sets, SortedSets}

/**
  * The ZIO-native surface: the same client, with every method returning `Task`.
  */
type SageClient = Client[Task]

extension (client: SageClient) {

  /**
    * Runs a read with client-side caching and a ZIO `Duration` TTL — the ZIO-native form of [[Client.cached]].
    */
  def cached[A](command: Command[A], ttl: Duration): Task[A] =
    client.cached(command, ttl.asFiniteDuration)

  /**
    * The full SCAN iteration: stops on the server's zero cursor, never on an empty page. SCAN may return a key more than once.
    */
  def scanAll[K: KeyCodec](
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  ): ZStream[Any, Throwable, K] =
    scanStream(cursor => client.run(Keys.scan[K](cursor, pattern, count, ofType)))

  /**
    * The full HSCAN iteration over field/value pairs: stops on the server's zero cursor, never on an empty page.
    */
  def hScanAll[K: KeyCodec, F: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): ZStream[Any, Throwable, (F, V)] =
    scanStream(cursor => client.run(Hashes.hScan[K, F, V](key, cursor, pattern, count)))

  /**
    * The full SSCAN iteration over set members: stops on the server's zero cursor, never on an empty page.
    */
  def sScanAll[K: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): ZStream[Any, Throwable, V] =
    scanStream(cursor => client.run(Sets.sScan[K, V](key, cursor, pattern, count)))

  /**
    * The full ZSCAN iteration over member/score pairs: stops on the server's zero cursor, never on an empty page.
    */
  def zScanAll[K: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): ZStream[Any, Throwable, (V, Double)] =
    scanStream(cursor => client.run(SortedSets.zScan[K, V](key, cursor, pattern, count)))

  private def scanStream[A](fetch: ScanCursor => Task[ScanPage[A]]): ZStream[Any, Throwable, A] =
    CStream
      .unfold[Option[ScanCursor], Vector[A]](Some(ScanCursor.start)) {
        case None         => CIO.value(None)
        case Some(cursor) => CIO.lift(fetch(cursor)).map(page => Some((page.items, page.next)))
      }
      .flatMap(items => CStream.init(items))
      .lower

  /**
    * Subscribes to one or more channels; closing the stream's scope unsubscribes. Survives reconnects via auto-resubscribe, dropping
    * messages published during the reconnect gap.
    */
  def subscribe[V: ValueCodec](channel: String, rest: String*): ZStream[Any, Throwable, Message[V]] =
    streamOf(client.subscribeChannels[V](channel, rest*))

  /**
    * Subscribes to one or more glob patterns; each delivery names the matching pattern and the concrete channel.
    */
  def pSubscribe[V: ValueCodec](pattern: String, rest: String*): ZStream[Any, Throwable, PatternMessage[V]] =
    streamOf(client.subscribePatterns[V](pattern, rest*))

  /**
    * Subscribes to one or more Shard Channels; in a cluster each is routed to the Node owning its Slot, and resubscription follows the Slot
    * on migration or failover. A sharded delivery is an ordinary [[Message]].
    */
  def sSubscribe[V: ValueCodec](channel: String, rest: String*): ZStream[Any, Throwable, Message[V]] =
    streamOf(client.subscribeShardChannels[V](channel, rest*))

  private def streamOf[A](open: Task[Subscription[Task, A]]): ZStream[Any, Throwable, A] =
    ZStream.unwrapScoped(
      ZIO.acquireRelease(open)(_.close.ignore).map { sub =>
        ZStream.repeatZIOOption(sub.next.mapError(Some(_)).flatMap {
          case Some(a) => ZIO.succeed(a)
          case None    => ZIO.fail(None)
        })
      }
    )
}

object SageClient {

  def connect(config: SageConfig): Task[SageClient] =
    Client.connect(config).lower.map(new Lowered(_))

  def scoped(config: SageConfig): ZIO[Scope, Throwable, SageClient] =
    ZIO.acquireRelease(connect(config))(_.close.ignore)

  def layer(config: SageConfig): ZLayer[Any, Throwable, SageClient] =
    ZLayer.scoped(scoped(config))

  final private class Lowered(underlying: Client[CIO]) extends Client[Task] {

    def run[A](command: Command[A]): Task[A] = underlying.run(command).lower

    def cached[A](command: Command[A], ttl: FiniteDuration): Task[A] = underlying.cached(command, ttl).lower

    def pipeline[Out, R](p: Pipeline[Out, R]): Task[Out] = underlying.pipeline(p).lower

    def pipelineAttempt[Out, R](p: Pipeline[Out, R]): Task[R] = underlying.pipelineAttempt(p).lower

    def transaction[A](body: TransactionScope[Task] => Task[A]): Task[A] =
      underlying.transaction[A](scope => CIO.lift(body(lower(scope)))).lower

    def subscribeChannels[V: ValueCodec](channel: String, rest: String*): Task[Subscription[Task, Message[V]]] =
      underlying.subscribeChannels[V](channel, rest*).map(lower).lower

    def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): Task[Subscription[Task, PatternMessage[V]]] =
      underlying.subscribePatterns[V](pattern, rest*).map(lower).lower

    def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): Task[Subscription[Task, Message[V]]] =
      underlying.subscribeShardChannels[V](channel, rest*).map(lower).lower

    private def lower(scope: TransactionScope[CIO]): TransactionScope[Task] =
      new TransactionScope[Task] {
        def watch[K: KeyCodec](key: K, rest: K*): Task[Unit]          = scope.watch(key, rest*).lower
        def run[A](command: Command[A]): Task[A]                      = scope.run(command).lower
        def exec[Out, R](p: Pipeline[Out, R]): Task[Option[Out]]      = scope.exec(p).lower
        def execAttempt[Out, R](p: Pipeline[Out, R]): Task[Option[R]] = scope.execAttempt(p).lower
        def discard: Task[Unit]                                       = scope.discard.lower
      }

    private def lower[A](sub: Subscription[CIO, A]): Subscription[Task, A] =
      new Subscription[Task, A] {
        def next: Task[Option[A]] = sub.next.lower
        def close: Task[Unit]     = sub.close.lower
      }

    def close: Task[Unit] = underlying.close.lower
  }
}

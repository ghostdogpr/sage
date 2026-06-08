package sage.ox

import scala.util.control.NonFatal

import _root_.ox.{useInScope, Ox}
import _root_.ox.flow.Flow
import kyo.compat.*

import sage.client.SageConfig
import sage.client.internal.{Client, TransactionScope}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Command, Hashes, Keys, Pipeline, RedisType, ScanCursor, Sets, SortedSets}

/**
  * The Ox-native surface: direct style, every method usable inside an Ox scope.
  */
type SageClient = Client[[A] =>> Ox ?=> A]

extension (client: SageClient) {

  /**
    * The full SCAN iteration: stops on the server's zero cursor, never on an empty page. SCAN may return a key more than once.
    */
  def scanAll[K: KeyCodec](
    pattern: Option[String] = None,
    count: Option[Long] = None,
    ofType: Option[RedisType] = None
  ): Ox ?=> Flow[K] =
    CStream
      .unfold[Option[ScanCursor], Vector[K]](Some(ScanCursor.start)) {
        case None         => CIO.value(None)
        case Some(cursor) =>
          CIO.lift(client.run(Keys.scan[K](cursor, pattern, count, ofType))).map(page => Some((page.items, page.next)))
      }
      .flatMap(keys => CStream.init(keys))
      .lower

  /**
    * The full HSCAN iteration over field/value pairs: stops on the server's zero cursor, never on an empty page.
    */
  def hScanAll[K: KeyCodec, F: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Ox ?=> Flow[(F, V)] =
    CStream
      .unfold[Option[ScanCursor], Vector[(F, V)]](Some(ScanCursor.start)) {
        case None         => CIO.value(None)
        case Some(cursor) =>
          CIO.lift(client.run(Hashes.hScan[K, F, V](key, cursor, pattern, count))).map(page => Some((page.items, page.next)))
      }
      .flatMap(pairs => CStream.init(pairs))
      .lower

  /**
    * The full SSCAN iteration over set members: stops on the server's zero cursor, never on an empty page.
    */
  def sScanAll[K: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Ox ?=> Flow[V] =
    CStream
      .unfold[Option[ScanCursor], Vector[V]](Some(ScanCursor.start)) {
        case None         => CIO.value(None)
        case Some(cursor) =>
          CIO.lift(client.run(Sets.sScan[K, V](key, cursor, pattern, count))).map(page => Some((page.items, page.next)))
      }
      .flatMap(members => CStream.init(members))
      .lower

  /**
    * The full ZSCAN iteration over member/score pairs: stops on the server's zero cursor, never on an empty page.
    */
  def zScanAll[K: KeyCodec, V: ValueCodec](
    key: K,
    pattern: Option[String] = None,
    count: Option[Long] = None
  ): Ox ?=> Flow[(V, Double)] =
    CStream
      .unfold[Option[ScanCursor], Vector[(V, Double)]](Some(ScanCursor.start)) {
        case None         => CIO.value(None)
        case Some(cursor) =>
          CIO.lift(client.run(SortedSets.zScan[K, V](key, cursor, pattern, count))).map(page => Some((page.items, page.next)))
      }
      .flatMap(pairs => CStream.init(pairs))
      .lower
}

object SageClient {

  def connect(config: SageConfig): Ox ?=> SageClient = new Lowered(Client.connect(config).lower)

  def scoped(config: SageConfig): Ox ?=> SageClient =
    useInScope(connect(config)) { client =>
      try client.close
      catch { case NonFatal(_) => () }
    }

  final private class Lowered(underlying: Client[CIO]) extends Client[[A] =>> Ox ?=> A] {

    def run[A](command: Command[A]): Ox ?=> A = underlying.run(command).lower

    def pipeline[Out, R](p: Pipeline[Out, R]): Ox ?=> Out = underlying.pipeline(p).lower

    def pipelineAttempt[Out, R](p: Pipeline[Out, R]): Ox ?=> R = underlying.pipelineAttempt(p).lower

    def transaction[A](body: TransactionScope[[X] =>> Ox ?=> X] => (Ox ?=> A)): Ox ?=> A =
      underlying.transaction[A](scope => CIO.lift(body(lower(scope)))).lower

    private def lower(scope: TransactionScope[CIO]): TransactionScope[[X] =>> Ox ?=> X] =
      new TransactionScope[[X] =>> Ox ?=> X] {
        def watch[K: KeyCodec](key: K, rest: K*): Ox ?=> Unit          = scope.watch(key, rest*).lower
        def run[A](command: Command[A]): Ox ?=> A                      = scope.run(command).lower
        def exec[Out, R](p: Pipeline[Out, R]): Ox ?=> Option[Out]      = scope.exec(p).lower
        def execAttempt[Out, R](p: Pipeline[Out, R]): Ox ?=> Option[R] = scope.execAttempt(p).lower
        def discard: Ox ?=> Unit                                       = scope.discard.lower
      }

    def close: Ox ?=> Unit = underlying.close.lower
  }
}

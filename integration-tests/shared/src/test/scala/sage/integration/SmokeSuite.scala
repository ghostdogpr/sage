package sage.integration

import scala.concurrent.duration.*

import kyo.compat.*

import sage.client.internal.Client

// A Pekko Future starts when it is built, so each later step is built inside flatMap instead of chained with >>.
abstract class SmokeSuite extends ServerSuite(Images.redis) {

  protected def lockScopesNativeEffects[F[_]](client: Client[F, String])(lift: [A] => F[A] => CIO[A]): CIO[Unit] = {
    val locks = client.lock[String]()
    lift(locks.withLock("native-lock", 2.seconds)(locks.tryWithLock("native-lock")(fail("contended body ran"))))
      .is(None)
      .flatMap(_ => lift(locks.tryWithLock("native-lock")(client.ping())).is(Some("PONG")))
  }

  protected def scanAllFindsEveryKey[F[_]](client: Client[F, String])(lift: [A] => F[A] => CIO[A])(scan: => F[Iterable[String]]): CIO[Unit] = {
    val keys = (1 to 50).map(i => s"scan-$i")
    inSequence(keys)(key => lift(client.set(key, "v"))).flatMap(_ => lift(scan).map(_.toSet).is(keys.toSet))
  }
}

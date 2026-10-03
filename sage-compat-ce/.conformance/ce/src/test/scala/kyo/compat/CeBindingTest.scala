package kyo.compat

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.*
import scala.util.Failure

class CeBindingTest extends CompatTest {

  private def failsWhileBuilding(): CIO[Int] = throw TestError("built")

  "ensure runs the cleanup when building the computation throws" in run {
    val ran = new AtomicInteger(0)
    CIO.ensure(CIO.defer { val _ = ran.incrementAndGet() })(failsWhileBuilding()).liftToTry.map {
      case Failure(TestError("built")) => assert(ran.get == 1)
      case other                       => fail(s"expected Failure(TestError(built)), got $other")
    }
  }

  "foreachIndexed numbers a Set's elements in iteration order" in run {
    val set = (1 to 20).map(i => s"e$i").toSet
    CIO.foreachIndexed(set)((i, a) => CIO.value(i -> a)).map(out => assert(out.toSeq == set.toSeq.zipWithIndex.map(_.swap)))
  }

  "timeout reports a null result as completed" in run {
    CIO.timeout(1.minute)(CIO.value(null: String)).map(out => assert(out == Some(null)))
  }

  "atomic compareAndSet compares values outside the boxing cache" in run {
    for {
      i     <- CAtomicInt.init(1000)
      okInt <- i.compareAndSet(Integer.valueOf(1000).intValue, 2000)
      l     <- CAtomicLong.init(1000L)
      okLng <- l.compareAndSet(java.lang.Long.valueOf(1000L).longValue, 2000L)
      ra    <- CAtomicRef.init[Any](1000L)
      okAny <- ra.compareAndSet(java.lang.Long.valueOf(1000L), 2000L)
      vi    <- i.get
      vl    <- l.get
      va    <- ra.get
    } yield assert(okInt && okLng && okAny && vi == 2000 && vl == 2000L && va == 2000L)
  }

  "atomic arithmetic wraps on overflow" in run {
    for {
      i  <- CAtomicInt.init(Int.MaxValue)
      vi <- i.incrementAndGet
      di <- i.getAndDecrement
      l  <- CAtomicLong.init(Long.MinValue)
      vl <- l.decrementAndGet
      al <- l.getAndAdd(2L)
      nl <- l.get
    } yield assert(vi == Int.MinValue && di == Int.MinValue && vl == Long.MaxValue && al == Long.MaxValue && nl == Long.MinValue + 1)
  }
}

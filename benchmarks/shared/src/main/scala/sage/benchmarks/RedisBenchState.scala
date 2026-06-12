package sage.benchmarks

import org.openjdk.jmh.annotations.{Level, Setup, TearDown}

/**
  * Shared JMH state for every cell's benchmarks: boots a Redis, builds the cell's clients (sage plus any competitors), seeds the data, and
  * tears it all down per trial. Concrete per-cell subclasses carry the `@State`, the `@Param` set, and the `@Benchmark` methods.
  */
abstract class RedisBenchState {

  val fixture: RedisFixture = new RedisFixture
  var subject: BenchClient  = null
  var keys: Array[String]   = Array.empty

  /**
    * The size of seeded values; subclasses with a `valueSize` param override this so GET reads what they will time.
    */
  protected def seedValueBytes: Int

  /**
    * The client under test this trial — its globally-unique name (e.g. `sage-zio`, `redis4cats`), so merged results self-identify.
    */
  protected def subjectName: String

  /**
    * The cell builds only the named client, so a trial holds no idle competitor connections/runtimes that could add noise.
    */
  protected def buildClient(host: String, port: Int, name: String): BenchClient

  @Setup(Level.Trial)
  def setupTrial(): Unit = {
    fixture.start()
    subject = buildClient(fixture.host, fixture.port, subjectName)
    keys = Payloads.keys("bench")
    subject.seed("bench", Payloads.KeyCount, Payloads.value(seedValueBytes), Payloads.HashKey, Payloads.HashFields)
  }

  @TearDown(Level.Trial)
  def tearDownTrial(): Unit = {
    if (subject != null) subject.close()
    fixture.stop()
  }
}

package sage.benchmarks

import org.openjdk.jmh.annotations.{Level, Setup, TearDown}

/**
  * Shared JMH state for every cell's benchmarks. It starts Redis, builds the cell's clients (Sage and any competitors), seeds the data, and
  * shuts everything down for each trial. Concrete subclasses define the `@State`, `@Param`, and `@Benchmark` annotations for their cell.
  */
abstract class RedisBenchState {

  val fixture: RedisFixture = new RedisFixture
  var subject: BenchClient  = null
  var keys: Array[String]   = Array.empty

  /**
    * The size of the seeded values. Subclasses with a `valueSize` parameter override this to match the values used by their GET benchmark.
    */
  protected def seedValueBytes: Int

  /**
    * The client under test for this trial. Its unique name, such as `sage-zio` or `redis4cats`, identifies it in merged results.
    */
  protected def subjectName: String

  /**
    * Builds only the named client. Other clients and their runtimes remain stopped during the trial and cannot affect the result.
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

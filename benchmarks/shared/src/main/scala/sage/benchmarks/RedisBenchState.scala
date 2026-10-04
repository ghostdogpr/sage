package sage.benchmarks

import java.util.concurrent.TimeUnit

import com.dimafeng.testcontainers.GenericContainer
import org.openjdk.jmh.annotations.*

/**
  * Shared JMH state for every cell's benchmarks. It starts Redis, seeds the data, builds the client under test, and shuts everything down for
  * each trial. Each cell defines a `client` `@Param` whose unique value, such as `sage-zio` or `redis4cats`, identifies the client in merged
  * results.
  */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
abstract class RedisBenchState {

  var subject: BenchClient            = null
  private var redis: GenericContainer = null

  /**
    * The size of the seeded values and of the values the SET benchmarks write.
    */
  def valueSize: Int

  def client: String

  lazy val value: String = "v" * valueSize

  protected def clusterEnabled: Boolean = false

  /**
    * Builds only the client under test. Other clients and their runtimes remain stopped during the trial and cannot affect the result.
    */
  protected def buildClient(host: String, port: Int): BenchClient = Clients.build(host, port, client)

  @Setup(Level.Trial)
  def setupTrial(): Unit = {
    redis = RedisFixture.start(clusterEnabled, value)
    subject = buildClient(redis.host, redis.mappedPort(6379))
  }

  @TearDown(Level.Trial)
  def tearDownTrial(): Unit = {
    if (subject != null) subject.close()
    if (redis != null) redis.stop()
  }
}

@OperationsPerInvocation(Payloads.KeyCount)
abstract class ThroughputWorkload extends RedisBenchState {

  @Param(Array("1", "8", "64", "256")) var concurrency: Int = 1

  lazy val work = new Payloads.Workload(concurrency)

  @Benchmark def get(): Unit = subject.getAll(work)
  @Benchmark def set(): Unit = subject.setAll(work, value)
}

abstract class ThroughputBenchBase extends ThroughputWorkload {

  @Param(Array("16", "1024")) var valueSize: Int = 16
}

// Run one large-reply command per invocation. The throughput benchmark covers concurrent requests. valueSize controls the seeded value size.
abstract class CollectionBenchBase extends RedisBenchState {

  @Param(Array("16")) var valueSize: Int = 16

  @Benchmark def mget(): Unit    = subject.mget()
  @Benchmark def hgetall(): Unit = subject.hgetall()
}

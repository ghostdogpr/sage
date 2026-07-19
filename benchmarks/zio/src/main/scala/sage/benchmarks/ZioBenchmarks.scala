package sage.benchmarks

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@OperationsPerInvocation(1000) // = Payloads.KeyCount
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
class ThroughputBench extends RedisBenchState {

  @Param(Array("sage-zio", "zio-redis")) var client: String = "sage-zio"
  @Param(Array("1", "8", "64", "256")) var concurrency: Int = 1
  @Param(Array("16", "1024")) var valueSize: Int            = 16

  protected def subjectName: String                                             = client
  override protected def seedValueBytes: Int                                    = valueSize
  protected def buildClient(host: String, port: Int, name: String): BenchClient = Clients.build(host, port, name)

  @Benchmark def get(): Long = subject.getAll(keys, concurrency)
  @Benchmark def set(): Long = subject.setAll(keys, Payloads.value(valueSize), concurrency)
}

// the throughput workload swept over the client topology (sage only): end-to-end overhead, server mode included — see TopologyBenchState
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@OperationsPerInvocation(1000) // = Payloads.KeyCount
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
class TopologyBench extends TopologyBenchState {

  @Param(Array("sage-zio")) var client: String                                  = "sage-zio"
  @Param(Array("standalone", "cluster", "master-replica")) var topology: String = "standalone"
  @Param(Array("1", "8", "64", "256")) var concurrency: Int                     = 1
  @Param(Array("16")) var valueSize: Int                                        = 16

  protected def topologyName: String                                            = topology
  override protected def seedValueBytes: Int                                    = valueSize
  protected def buildClient(host: String, port: Int, name: String): BenchClient = Clients.buildTopology(host, port, name)

  @Benchmark def get(): Long = subject.getAll(keys, concurrency)
  @Benchmark def set(): Long = subject.setAll(keys, Payloads.value(valueSize), concurrency)
}

// a single big-reply command per invocation (no concurrency — that's the throughput workload); valueSize sizes the seeded values
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
class CollectionBench extends RedisBenchState {

  @Param(Array("sage-zio", "zio-redis")) var client: String = "sage-zio"
  @Param(Array("16")) var valueSize: Int                    = 16

  protected def subjectName: String                                             = client
  override protected def seedValueBytes: Int                                    = valueSize
  protected def buildClient(host: String, port: Int, name: String): BenchClient = Clients.build(host, port, name)

  @Benchmark def mget(): Long    = subject.mget(keys)
  @Benchmark def hgetall(): Long = subject.hgetall(Payloads.HashKey)
}

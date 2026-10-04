package sage.benchmarks

object Clients {
  def build(host: String, port: Int, name: String): BenchClient = throw new IllegalArgumentException(s"unknown client: $name")
}

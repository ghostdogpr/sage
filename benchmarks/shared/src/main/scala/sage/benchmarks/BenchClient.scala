package sage.benchmarks

/**
  * One client under benchmark. Every method runs its effect to completion (lowered/blocked) so JMH measures real round-trip work, and every
  * method returns a checksum the benchmark consumes — both to report meaningful work and to stop the JIT from eliminating the call.
  */
trait BenchClient extends AutoCloseable {

  def name: String

  /**
    * Seed `count` string keys `prefix:0..count-1` with `value`, plus one hash `hashKey` of `fields` field/value pairs.
    */
  def seed(prefix: String, count: Int, value: String, hashKey: String, fields: Int): Unit

  /**
    * GET every key with `concurrency` commands in flight; returns the total length of the values read.
    */
  def getAll(keys: Array[String], concurrency: Int): Long

  /**
    * SET every key to `value` with `concurrency` commands in flight; returns the number of writes.
    */
  def setAll(keys: Array[String], value: String, concurrency: Int): Long

  /**
    * One MGET of all `keys`; returns the total length of the values read.
    */
  def mget(keys: Array[String]): Long

  /**
    * One HGETALL of `key`; returns the number of fields read.
    */
  def hgetall(key: String): Long
}

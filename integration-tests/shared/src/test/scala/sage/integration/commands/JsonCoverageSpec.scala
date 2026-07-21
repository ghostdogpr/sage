package sage.integration.commands

import scala.concurrent.ExecutionContext

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.munit.TestContainersForAll
import kyo.compat.*

import sage.client.SageConfig
import sage.commands.CommandSamples
import sage.integration.Images

/**
  * The extension-aware coverage spec for the JSON module. It runs the module-bearing images (Redis, which bundles RedisJSON, and Valkey Bundle,
  * which ships valkey-json), takes the union of every `JSON.*` command each server reports, and requires an exact partition against the
  * implemented samples plus [[JsonCoverage.skipped]]. Unlike the core spec it does not drop subcommand names, so each `JSON.DEBUG` subcommand
  * must be implemented or explicitly skipped and a new one fails until acknowledged. Redis-only commands are covered through Redis and
  * Valkey-only ones through the bundle.
  */
class JsonCoverageSpec extends munit.FunSuite with TestContainersForAll with CoverageSupport {

  override type Containers = GenericContainer and GenericContainer

  override def startContainers(): Containers =
    GenericContainer.Def(Images.redis, exposedPorts = Seq(6379)).start() and
      GenericContainer.Def(Images.valkeyBundle, exposedPorts = Seq(6379)).start()

  given ExecutionContext = munitExecutionContext

  private val implemented: Set[String] = CommandSamples.all.map(_.command.name).toSet.filter(_.startsWith("JSON."))

  test("implemented JSON commands never overlap the acknowledged gaps") {
    assertEquals(implemented.intersect(JsonCoverage.skipped.keySet), Set.empty[String])
  }

  test("the JSON partition is exact per backend and the backend differences are acknowledged") {
    withContainers { case redis and valkey =>
      (for {
        redisJson  <- jsonCommands(configOf(redis))
        valkeyJson <- jsonCommands(configOf(valkey))
      } yield {
        assertExactPartition("JSON", redisJson ++ valkeyJson, implemented, JsonCoverage.skipped.keySet)
        assertEquals(redisJson -- valkeyJson, JsonCoverage.redisOnly, "Redis-only JSON commands drifted from the acknowledged set")
        assertEquals(valkeyJson -- redisJson, JsonCoverage.valkeyOnly, "Valkey-only JSON commands drifted from the acknowledged set")
      }).unsafeRun
    }
  }

  private def jsonCommands(config: SageConfig): CIO[Set[String]] =
    connectAndUse(config)(_.run(commandList).map(_.toSet.filter(_.startsWith("JSON."))))
}

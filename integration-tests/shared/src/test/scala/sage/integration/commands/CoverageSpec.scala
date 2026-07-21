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
  * Diffs the implemented commands (the core sample fixtures) against the command list each live server reports, after subtracting module
  * commands. The partition must be exact: every drift fails with the offending names. The JSON extension family is a loadable module the
  * subtraction removes here; it is validated separately by [[JsonCoverageSpec]].
  */
class CoverageSpec extends munit.FunSuite with TestContainersForAll with CoverageSupport {

  override type Containers = GenericContainer and GenericContainer

  override def startContainers(): Containers =
    GenericContainer.Def(Images.redis, exposedPorts = Seq(6379)).start() and
      GenericContainer.Def(Images.valkey, exposedPorts = Seq(6379)).start()

  given ExecutionContext = munitExecutionContext

  private val implemented: Set[String] = CommandSamples.all.map(_.command.name).toSet.filterNot(_.startsWith("JSON."))

  test("implemented commands never overlap the acknowledged gaps") {
    assertEquals(implemented.intersect(Coverage.skipped.keySet), Set.empty[String])
  }

  test("the partition is exact against both live servers") {
    withContainers { case redis and valkey =>
      (for {
        redisCore  <- coreCommands(configOf(redis))
        valkeyCore <- coreCommands(configOf(valkey))
      } yield {
        // a subcommand modeled as an argument is covered by its bare command; only space-containing names sage implements as their own
        // Command name (XINFO/XGROUP) are tracked
        val serverUnion = (redisCore ++ valkeyCore).filterNot(name => name.contains(' ') && !implemented.contains(name))
        assertExactPartition("core", serverUnion, implemented, Coverage.skipped.keySet)
        report("redis", redisCore)
        report("valkey", valkeyCore)
      }).unsafeRun
    }
  }

  private def report(server: String, core: Set[String]): Unit =
    println(
      s"[coverage] $server: ${core.size} commands, ${core.intersect(implemented).size} implemented, " +
        s"${core.intersect(Coverage.skipped.keySet).size} skipped"
    )

  private def coreCommands(config: SageConfig): CIO[Set[String]] =
    connectAndUse(config) { client =>
      for {
        all     <- client.run(commandList)
        modules <- client.run(moduleNames)
        module  <- modules.foldLeft(CIO.value(Set.empty[String])) { (acc, name) =>
                     acc.flatMap(commands => client.run(commandListForModule(name)).map(commands ++ _))
                   }
      } yield all.toSet -- module
    }
}

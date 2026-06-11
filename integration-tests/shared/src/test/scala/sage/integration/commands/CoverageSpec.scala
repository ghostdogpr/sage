package sage.integration.commands

import scala.concurrent.ExecutionContext

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.munit.TestContainersForAll
import kyo.compat.*

import sage.Bytes
import sage.SageException.DecodeError
import sage.client.SageConfig
import sage.client.internal.Client
import sage.commands.{Command, CommandSamples}
import sage.integration.Images
import sage.protocol.Frame

/**
  * Diffs the implemented commands (the core sample fixtures) against the command list each live server reports, after subtracting module
  * commands. The partition must be exact: every drift fails with the offending names.
  */
class CoverageSpec extends munit.FunSuite with TestContainersForAll {

  override type Containers = GenericContainer and GenericContainer

  override def startContainers(): Containers =
    GenericContainer.Def(Images.redis, exposedPorts = Seq(6379)).start() and
      GenericContainer.Def(Images.valkey, exposedPorts = Seq(6379)).start()

  given ExecutionContext = munitExecutionContext

  private val implemented: Set[String] = CommandSamples.all.map(_.command.name).toSet

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

        val unacknowledged = serverUnion -- implemented -- Coverage.skipped.keySet
        assert(unacknowledged.isEmpty, s"unacknowledged server commands: ${unacknowledged.toVector.sorted.mkString(", ")}")

        val unknownImplemented = implemented -- serverUnion
        assert(unknownImplemented.isEmpty, s"implemented commands unknown to both servers: ${unknownImplemented.toVector.sorted.mkString(", ")}")

        val stale = Coverage.skipped.keySet -- serverUnion
        assert(stale.isEmpty, s"skipped entries unknown to both servers: ${stale.toVector.sorted.mkString(", ")}")

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

  private def configOf(server: GenericContainer): SageConfig =
    SageConfig(host = server.host, port = server.mappedPort(6379))

  private def coreCommands(config: SageConfig): CIO[Set[String]] =
    Client.connect(config).flatMap { client =>
      (for {
        all     <- client.run(commandList)
        modules <- client.run(moduleNames)
        module  <- modules.foldLeft(CIO.value(Set.empty[String])) { (acc, name) =>
                     acc.flatMap(commands => client.run(commandListForModule(name)).map(commands ++ _))
                   }
      } yield all.toSet -- module).fold(
        result => client.close.map(_ => result),
        error => client.close.flatMap(_ => CIO.fail(error))
      )
    }

  private val commandList: Command[Vector[String]] = rawCommandList(Vector(Bytes.utf8("LIST")))

  private def commandListForModule(module: String): Command[Vector[String]] =
    rawCommandList(Vector("LIST", "FILTERBY", "MODULE", module).map(Bytes.utf8))

  private def rawCommandList(args: Vector[Bytes]): Command[Vector[String]] =
    Command(
      "COMMAND",
      keyIndices = Command.NoKeys,
      args = args,
      decode = {
        case Frame.Array(elements) =>
          elements.foldLeft[Either[DecodeError, Vector[String]]](Right(Vector.empty)) { (acc, frame) =>
            acc.flatMap { names =>
              frame match {
                case Frame.BulkString(name) => Right(names :+ normalize(name.asUtf8String))
                case other                  => Left(DecodeError("bulk string command name", Frame.describe(other)))
              }
            }
          }
        case other                 => Left(DecodeError("array of command names", Frame.describe(other)))
      }
    )

  private val moduleNames: Command[Vector[String]] =
    Command(
      "MODULE",
      keyIndices = Command.NoKeys,
      args = Vector(Bytes.utf8("LIST")),
      decode = {
        case Frame.Array(modules) =>
          Right(modules.collect { case Frame.Map(entries) =>
            entries.collectFirst {
              case (Frame.BulkString(key), Frame.BulkString(value)) if key.asUtf8String == "name" =>
                value.asUtf8String
            }
          }.flatten)
        case other                => Left(DecodeError("array of module maps", Frame.describe(other)))
      }
    )

  // the server reports lowercase names with pipe-separated subcommands; Command names are uppercase words
  private def normalize(name: String): String = name.toUpperCase.replace('|', ' ')
}

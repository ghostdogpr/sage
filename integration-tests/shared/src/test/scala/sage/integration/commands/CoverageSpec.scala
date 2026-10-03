package sage.integration.commands

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.munit.TestContainersForAll
import kyo.compat.*

import sage.Bytes
import sage.SageException.DecodeError
import sage.client.SageConfig
import sage.client.internal.Client
import sage.commands.{Command, CommandFilterBy, CommandSamples}
import sage.integration.{ContainerClient, Images}
import sage.protocol.Frame

/**
  * Diffs the implemented commands (the sample fixtures) against the command list each live server reports. The partition must be exact: every
  * drift fails with the offending names.
  *
  * Module commands are subtracted except JSON, which the module-bearing images report (Redis bundles RedisJSON, and Valkey Bundle ships
  * valkey-json). JSON names keep their subcommands, so a new `JSON.DEBUG` subcommand fails until acknowledged.
  */
class CoverageSpec extends ContainerClient with TestContainersForAll {

  override type Containers = GenericContainer and GenericContainer and GenericContainer

  override def startContainers(): Containers =
    serverDef(Images.redis).start() and serverDef(Images.valkey).start() and serverDef(Images.valkeyBundle).start()

  private val implemented = CommandSamples.all.map(_.command.name).toSet

  private def json(names: Set[String]): Set[String] = names.filter(_.startsWith("JSON."))

  test("implemented commands never overlap the acknowledged gaps") {
    assertEquals(implemented.intersect(Coverage.skipped.keySet), Set.empty[String])
  }

  containerTest("the partition is exact against every live server and the JSON backend differences are acknowledged") {
    case redis and valkey and bundle =>
      for {
        redisListing  <- listing(configOf(redis))
        valkeyListing <- listing(configOf(valkey))
        bundleListing <- listing(configOf(bundle))
      } yield {
        // Count a subcommand modeled as an argument under its base command. Track a space-separated command name only when Sage implements
        // that complete name, as it does for XINFO and XGROUP.
        val serverUnion =
          (redisListing ++ valkeyListing ++ bundleListing).filterNot(name => name.contains(' ') && !name.startsWith("JSON.") && !implemented(name))
        assertExactPartition(serverUnion, implemented, Coverage.skipped.keySet)
        report("redis", redisListing)
        report("valkey", valkeyListing)

        val (redisJson, valkeyJson) = (json(redisListing), json(bundleListing))
        assertEquals(redisJson -- valkeyJson, Coverage.redisOnly, "Redis-only JSON commands drifted from the acknowledged set")
        assertEquals(valkeyJson -- redisJson, json(serverUnion).filter(_.startsWith("JSON.DEBUG ")), "only valkey-json lists JSON.DEBUG subcommands")
      }
  }

  private def report(server: String, names: Set[String]): Unit =
    println(
      s"[coverage] $server: ${names.size} commands, ${names.intersect(implemented).size} implemented, " +
        s"${names.intersect(Coverage.skipped.keySet).size} skipped"
    )

  private def listing(config: SageConfig): CIO[Set[String]] =
    connectAndUse(config) { client =>
      for {
        all     <- commandList(client)
        modules <- client.run(moduleNames)
        module  <- CIO.foreach(modules)(name => commandList(client, Some(CommandFilterBy.Module(name)))).map(_.iterator.flatten.toSet)
      } yield all -- module ++ json(all)
    }

  private def commandList(client: Client[CIO, String], filterBy: Option[CommandFilterBy] = None): CIO[Set[String]] =
    client.commandList(filterBy).map(_.iterator.map(normalize).toSet)

  private val moduleNames: Command[Vector[String]] =
    Command(
      "MODULE",
      keyIndices = Command.NoKeys,
      args = Vector(Bytes.utf8("LIST")),
      decode = {
        case Frame.Array(modules) =>
          Right(modules.collect { case Frame.Map(entries) =>
            entries.collectFirst {
              case (Frame.BulkString(key), Frame.BulkString(value)) if key.asUtf8String == "name" => value.asUtf8String
            }
          }.flatten)
        case other                => Left(DecodeError("array of module maps", Frame.describe(other)))
      }
    )

  // the server reports lowercase names with pipe-separated subcommands; Command names are uppercase words
  private def normalize(name: String): String = name.toUpperCase.replace('|', ' ')

  private def assertExactPartition(serverUnion: Set[String], implemented: Set[String], skipped: Set[String]): Unit = {
    val unacknowledged = serverUnion -- implemented -- skipped
    assert(unacknowledged.isEmpty, s"unacknowledged server commands: ${unacknowledged.toVector.sorted.mkString(", ")}")

    val unknownImplemented = implemented -- serverUnion
    assert(unknownImplemented.isEmpty, s"implemented commands unknown to both servers: ${unknownImplemented.toVector.sorted.mkString(", ")}")

    val stale = skipped -- serverUnion
    assert(stale.isEmpty, s"skipped entries unknown to both servers: ${stale.toVector.sorted.mkString(", ")}")
  }
}

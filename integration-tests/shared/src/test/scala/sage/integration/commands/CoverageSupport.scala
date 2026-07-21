package sage.integration.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.commands.Command
import sage.integration.ContainerClient
import sage.protocol.Frame

/**
  * Shared machinery for the command-coverage specs: the `COMMAND LIST` decoders, name normalization, and the exact-partition assertion. The
  * connect/teardown and config helpers come from [[ContainerClient]]. The core and JSON specs supply only their images, filters, and skip maps.
  */
trait CoverageSupport extends ContainerClient { this: munit.FunSuite =>

  protected val commandList: Command[Vector[String]] = rawCommandList(Vector(Bytes.utf8("LIST")))

  protected def commandListForModule(module: String): Command[Vector[String]] =
    rawCommandList(Vector("LIST", "FILTERBY", "MODULE", module).map(Bytes.utf8))

  protected val moduleNames: Command[Vector[String]] =
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
  protected def normalize(name: String): String = name.toUpperCase.replace('|', ' ')

  protected def assertExactPartition(label: String, serverUnion: Set[String], implemented: Set[String], skipped: Set[String]): Unit = {
    val unacknowledged = serverUnion -- implemented -- skipped
    assert(unacknowledged.isEmpty, s"$label unacknowledged server commands: ${unacknowledged.toVector.sorted.mkString(", ")}")

    val unknownImplemented = implemented -- serverUnion
    assert(unknownImplemented.isEmpty, s"$label implemented commands unknown to both servers: ${unknownImplemented.toVector.sorted.mkString(", ")}")

    val stale = skipped -- serverUnion
    assert(stale.isEmpty, s"$label skipped entries unknown to both servers: ${stale.toVector.sorted.mkString(", ")}")
  }

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
}

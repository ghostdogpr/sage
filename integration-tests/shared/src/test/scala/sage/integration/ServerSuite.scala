package sage.integration

import scala.concurrent.{ExecutionContext, Future}

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import kyo.compat.*

import sage.client.SageConfig
import sage.client.internal.Client

abstract class ServerSuite(image: String) extends munit.FunSuite with TestContainerForAll {

  override val containerDef: GenericContainer.Def[GenericContainer] = GenericContainer.Def(image, exposedPorts = Seq(6379))

  protected def configOf(server: GenericContainer): SageConfig =
    SageConfig(host = server.host, port = server.mappedPort(6379))

  // not private: only the Ox cell's unsafeRun consumes it, and a private given would be flagged unused on the other cells
  given ExecutionContext = munitExecutionContext

  protected def withClient[A](body: Client[CIO] => CIO[A]): Future[A] =
    withContainers(server => connectAndUse(configOf(server))(body).unsafeRun)

  // CIO.acquireReleaseWith fails to compile on the Ox/Future cells when its type argument nests CIO (Client[CIO]); fold instead
  protected def connectAndUse[A](config: SageConfig)(body: Client[CIO] => CIO[A]): CIO[A] =
    Client.connect(config).flatMap { client =>
      body(client).fold(
        result => client.close.map(_ => result),
        error => client.close.flatMap(_ => CIO.fail(error))
      )
    }
}

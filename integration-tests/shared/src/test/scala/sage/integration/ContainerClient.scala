package sage.integration

import com.dimafeng.testcontainers.GenericContainer
import kyo.compat.*

import sage.client.{Endpoint, SageConfig, Topology}
import sage.client.internal.Client

/**
  * Shared connect-and-teardown helpers for the testcontainers suites: build a config for a started container, and run a body against a
  * client that is always closed afterwards.
  */
trait ContainerClient {

  protected def configOf(server: GenericContainer): SageConfig =
    SageConfig(topology = Topology.Standalone(Endpoint(server.host, server.mappedPort(6379))))

  // CIO.acquireReleaseWith fails to compile on the Ox/Future cells when its type argument nests CIO (Client[CIO, String]); fold instead
  protected def connectAndUse[A](config: SageConfig)(body: Client[CIO, String] => CIO[A]): CIO[A] =
    Client.connect(config).flatMap { client =>
      body(client).fold(
        result => client.close.map(_ => result),
        error => client.close.flatMap(_ => CIO.fail(error))
      )
    }
}

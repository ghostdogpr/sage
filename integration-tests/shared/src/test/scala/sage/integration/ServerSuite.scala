package sage.integration

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll

import sage.client.SageConfig

abstract class ServerSuite(image: String) extends munit.FunSuite with TestContainerForAll {

  override val containerDef: GenericContainer.Def[GenericContainer] = GenericContainer.Def(image, exposedPorts = Seq(6379))

  protected def configOf(server: GenericContainer): SageConfig =
    SageConfig(host = server.host, port = server.mappedPort(6379))
}

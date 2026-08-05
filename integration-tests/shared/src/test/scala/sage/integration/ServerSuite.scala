package sage.integration

import scala.concurrent.{ExecutionContext, Future}

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import kyo.compat.*

import sage.client.internal.Client

abstract class ServerSuite(image: String) extends munit.FunSuite with TestContainerForAll with ContainerClient {

  override val containerDef: GenericContainer.Def[GenericContainer] = GenericContainer.Def(image, exposedPorts = Seq(6379))

  // The Ox cell's unsafeRun uses this value. Keeping it non-private avoids unused-private warnings in the other cells.
  given ExecutionContext = munitExecutionContext

  protected def withClient[A](body: Client[CIO, String] => CIO[A]): Future[A] =
    withContainers(server => connectAndUse(configOf(server))(body).unsafeRun)
}

package sage.integration

import scala.concurrent.{ExecutionContext, Future}

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import kyo.compat.*

import sage.client.internal.Client

abstract class ServerSuite(image: String) extends munit.FunSuite with TestContainerForAll with ContainerClient {

  override val containerDef: GenericContainer.Def[GenericContainer] = GenericContainer.Def(image, exposedPorts = Seq(6379))

  // not private: only the Ox cell's unsafeRun consumes it, and a private given would be flagged unused on the other cells
  given ExecutionContext = munitExecutionContext

  protected def withClient[A](body: Client[CIO] => CIO[A]): Future[A] =
    withContainers(server => connectAndUse(configOf(server))(body).unsafeRun)
}

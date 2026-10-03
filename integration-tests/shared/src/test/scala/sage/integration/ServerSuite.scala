package sage.integration

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.munit.{TestContainerForAll, TestContainersForAll}
import kyo.compat.*
import munit.{Location, TestOptions}

import sage.client.internal.Client

trait ServerTests extends ContainerClient {

  protected def serverTest(options: TestOptions)(body: GenericContainer => CIO[Any])(using Location): Unit

  protected def clientTest(options: TestOptions)(body: Client[CIO, String] => CIO[Any])(using Location): Unit =
    serverTest(options)(server => connectAndUse(configOf(server))(body))

  protected def clientsTest(options: TestOptions)(body: (Client[CIO, String], Client[CIO, String]) => CIO[Any])(using Location): Unit =
    serverTest(options)(server => connectAndUse(configOf(server))(first => connectAndUse(configOf(server))(body(first, _))))
}

abstract class ServerSuite(image: String) extends ServerTests with TestContainerForAll {

  override val containerDef: GenericContainer.Def[GenericContainer] = serverDef(image)

  protected def serverTest(options: TestOptions)(body: GenericContainer => CIO[Any])(using Location): Unit = containerTest(options)(body)
}

abstract class BothServersSuite extends ServerTests with TestContainersForAll {

  override type Containers = GenericContainer and GenericContainer

  protected def redisDef: GenericContainer.Def[GenericContainer]  = serverDef(Images.redis)
  protected def valkeyDef: GenericContainer.Def[GenericContainer] = serverDef(Images.valkey)

  override def startContainers(): Containers = redisDef.start() and valkeyDef.start()

  protected def serverTest(options: TestOptions)(body: GenericContainer => CIO[Any])(using Location): Unit = {
    onRedis(options)(body)
    onValkey(options)(body)
  }

  protected def onRedis(options: TestOptions)(body: GenericContainer => CIO[Any])(using Location): Unit =
    containerTest(options.withName(s"${options.name} (redis)")) { case redis and _ => body(redis) }

  protected def onValkey(options: TestOptions)(body: GenericContainer => CIO[Any])(using Location): Unit =
    containerTest(options.withName(s"${options.name} (valkey)")) { case _ and valkey => body(valkey) }

  protected def redisTest(options: TestOptions)(body: Client[CIO, String] => CIO[Any])(using Location): Unit =
    onRedis(options)(server => connectAndUse(configOf(server))(body))

  protected def valkeyTest(options: TestOptions)(body: Client[CIO, String] => CIO[Any])(using Location): Unit =
    onValkey(options)(server => connectAndUse(configOf(server))(body))
}

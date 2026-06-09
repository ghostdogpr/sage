package sage.integration.security

import scala.concurrent.{ExecutionContext, Future}

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import kyo.compat.*
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable

import sage.SageException.{ServerError, TlsError}
import sage.client.{AuthConfig, SageConfig, TlsConfig, TrustSource}
import sage.integration.{ContainerClient, Images}

/**
  * One TLS+ACL server hosts every case. The args start with `-`, so the redis and valkey entrypoints each prepend their own server
  * binary; `--port 0` makes the single exposed port speak only TLS. The cert is generated per run with the Docker host in its SAN
  * ([[TlsFixture]]), so hostname verification passes whatever host Testcontainers reports.
  */
abstract class TlsAuthSuite(image: String) extends munit.FunSuite with TestContainerForAll with ContainerClient {

  // certs are copied in over the Docker API rather than bind-mounted, so the suite works against a remote daemon too (a bind mount would
  // look for the path on the daemon host, not the test runner)
  override val containerDef: GenericContainer.Def[GenericContainer] =
    new GenericContainer.Def[GenericContainer]({
      val container = GenericContainer(
        image,
        exposedPorts = Seq(6379),
        command = Seq(
          "--tls-port",
          "6379",
          "--port",
          "0",
          "--tls-cert-file",
          "/tls/server.crt",
          "--tls-key-file",
          "/tls/server.key",
          "--tls-ca-cert-file",
          "/tls/server.crt",
          "--tls-auth-clients",
          "no",
          "--user",
          "default",
          "on",
          ">defaultpass",
          "~*",
          "+@all",
          "--user",
          "app",
          "on",
          ">apppass",
          "~*",
          "+@all"
        ),
        waitStrategy = Wait.forLogMessage(".*Ready to accept connections.*", 1)
      )
      container.underlyingUnsafeContainer
        .withCopyToContainer(Transferable.of(TlsFixture.serverCertPem), "/tls/server.crt")
        .withCopyToContainer(Transferable.of(TlsFixture.serverKeyPem), "/tls/server.key")
      container
    }) {}

  given ExecutionContext = munitExecutionContext

  private val caPath = TlsFixture.serverCert
  private val app    = AuthConfig(username = "app", password = "apppass")

  private def configWith(server: GenericContainer, trust: TrustSource = TrustSource.System, auth: AuthConfig = app): SageConfig =
    configOf(server).copy(tls = Some(TlsConfig(trust)), auth = Some(auth))

  private def connectAndPing(config: SageConfig): Future[String] = connectAndUse(config)(_.ping()).unsafeRun

  test("rejects the server certificate by default: the private CA is not in the system trust store") {
    withContainers { server =>
      connectAndPing(configWith(server)).failed.map(error => assert(error.isInstanceOf[TlsError], error))
    }
  }

  test("connects when the private CA is supplied as PEM trust material") {
    withContainers { server =>
      connectAndPing(configWith(server, TrustSource.Pem(caPath))).map(pong => assertEquals(pong, "PONG"))
    }
  }

  test("connects with verification disabled") {
    withContainers { server =>
      connectAndPing(configWith(server, TrustSource.Insecure)).map(pong => assertEquals(pong, "PONG"))
    }
  }

  test("ACL auth over TLS succeeds for a named user via HELLO, and round-trips a command") {
    withContainers { server =>
      connectAndUse(configWith(server, TrustSource.Pem(caPath))) { client =>
        for {
          _     <- client.set("tls:key", "value")
          value <- client.get[String, String]("tls:key")
        } yield value
      }.unsafeRun.map(value => assertEquals(value, Some("value")))
    }
  }

  test("bad credentials fail with a server error") {
    withContainers { server =>
      val config = configWith(server, TrustSource.Pem(caPath), AuthConfig(username = "app", password = "wrong"))
      connectAndPing(config).failed.map(error => assert(error.isInstanceOf[ServerError], error))
    }
  }
}

class RedisTlsAuthSuite extends TlsAuthSuite(Images.redis)

class ValkeyTlsAuthSuite extends TlsAuthSuite(Images.valkey)

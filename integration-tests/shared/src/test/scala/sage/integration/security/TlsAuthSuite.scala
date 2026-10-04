package sage.integration.security

import com.dimafeng.testcontainers.GenericContainer
import kyo.compat.*
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable

import sage.SageException.{ServerError, TlsError}
import sage.client.{AuthConfig, SageConfig, TlsConfig, TrustSource}
import sage.integration.{BothServersSuite, Images}

/**
  * One TLS+ACL server per image hosts every case. The args start with `-`, so the redis and valkey entrypoints each prepend their own server
  * binary; `--port 0` makes the single exposed port speak only TLS. The cert is generated per run with the Docker host in its SAN
  * ([[TlsFixture]]), so hostname verification passes whatever host Testcontainers reports.
  */
class TlsAuthSuite extends BothServersSuite {

  override protected def redisDef: GenericContainer.Def[GenericContainer]  = tlsServer(Images.redis)
  override protected def valkeyDef: GenericContainer.Def[GenericContainer] = tlsServer(Images.valkey)

  // Copy certificates through the Docker API so the suite also works with a remote daemon. A bind mount would look for the path on the daemon
  // host instead of the test runner.
  private def tlsServer(image: String): GenericContainer.Def[GenericContainer] =
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
        .withCopyToContainer(Transferable.of(TlsFixture.material.certPem), "/tls/server.crt")
        .withCopyToContainer(Transferable.of(TlsFixture.material.keyPem), "/tls/server.key")
      container
    }) {}

  private val caPath = TlsFixture.material.certFile
  private val app    = AuthConfig(username = "app", password = "apppass")

  private def configWith(server: GenericContainer, trust: TrustSource = TrustSource.System, auth: AuthConfig = app): SageConfig =
    configOf(server).copy(tls = Some(TlsConfig(trust)), auth = Some(auth))

  serverTest("rejects the server certificate by default: the private CA is not in the system trust store") { server =>
    failsWith[TlsError](connectAndUse(configWith(server))(_.ping()))
  }

  serverTest("connects with verification disabled") { server =>
    connectAndUse(configWith(server, TrustSource.Insecure))(_.ping().is("PONG"))
  }

  serverTest("ACL auth over TLS succeeds for a named user via HELLO, and round-trips a command") { server =>
    connectAndUse(configWith(server, TrustSource.Pem(caPath))) { client =>
      client.set("tls:key", "value").flatMap(_ => client.get[String]("tls:key").is(Some("value")))
    }
  }

  serverTest("bad credentials fail with a server error") { server =>
    failsWith[ServerError](
      connectAndUse(configWith(server, TrustSource.Pem(caPath), AuthConfig(username = "app", password = "wrong")))(_.ping())
    )
  }
}

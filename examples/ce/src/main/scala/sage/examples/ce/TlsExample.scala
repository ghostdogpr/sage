package sage.examples.ce

import cats.effect.IO

import sage.*
import sage.ce.*

/**
  * TLS + ACL spotlight. Both are pure configuration on top of the same client: `tls` selects the trust source (here the system trust store;
  * use `TrustSource.Pem`/`TrustStore` for a private CA, never `Insecure` outside development) and `auth` carries the ACL user. This needs a
  * TLS-enabled, ACL-protected server, so it is not part of the localhost `Tour` — it exists to show the wiring.
  */
object TlsExample {

  private val config =
    SageConfig(
      topology = Topology.Standalone(Endpoint("localhost", 6380)),
      tls = Some(TlsConfig(TrustSource.System)),
      auth = Some(AuthConfig(username = "app", password = "app-secret"))
    )

  def run: IO[String] =
    SageClient.resource(config).use(_.ping())
}

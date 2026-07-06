package sage.client.internal

import java.net.{InetAddress, InetSocketAddress, Socket}
import java.nio.file.{Files, Path}
import java.security.KeyStore
import javax.net.ssl.{KeyManagerFactory, SSLContext, SSLServerSocket, TrustManagerFactory}

import sage.SageException.TlsError
import sage.client.{TlsConfig, TrustSource}

class TlsSpec extends munit.FunSuite {

  // one self-signed cert whose only SAN is dns:localhost: the server presents it, the client trusts it, so only the connect host varies
  private lazy val material = certMaterial()

  private def certMaterial(): (SSLContext, SSLContext) = {
    val dir     = Files.createTempDirectory("sage-tls-unit")
    val store   = dir.resolve("server.p12")
    val pass    = "changeit".toCharArray
    val keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString
    val proc    = new ProcessBuilder(
      keytool,
      "-genkeypair",
      "-alias",
      "server",
      "-keyalg",
      "RSA",
      "-keysize",
      "2048",
      "-validity",
      "3650",
      "-storetype",
      "PKCS12",
      "-keystore",
      store.toString,
      "-storepass",
      "changeit",
      "-dname",
      "CN=localhost",
      "-ext",
      "san=dns:localhost"
    ).redirectErrorStream(true).start()
    val out     = new String(proc.getInputStream.readAllBytes())
    assert(proc.waitFor() == 0, s"keytool failed: $out")

    val ks = KeyStore.getInstance("PKCS12")
    val in = Files.newInputStream(store)
    try ks.load(in, pass)
    finally in.close()

    val kmf    = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(ks, pass)
    val server = SSLContext.getInstance("TLS")
    server.init(kmf.getKeyManagers, null, null)

    val trust  = KeyStore.getInstance(KeyStore.getDefaultType)
    trust.load(null, null)
    trust.setCertificateEntry("server", ks.getCertificate("server"))
    val tmf    = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(trust)
    val client = SSLContext.getInstance("TLS")
    client.init(null, tmf.getTrustManagers, null)
    (server, client)
  }

  private def withServer(body: Int => Unit): Unit = {
    val socket = material._1.getServerSocketFactory
      .createServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))
      .asInstanceOf[SSLServerSocket]
    socket.setSoTimeout(10000)
    val accept = Thread.ofVirtual().start { () =>
      try { val s = socket.accept(); s.getInputStream.read(); s.close() }
      catch { case _: Throwable => () }
    }
    try body(socket.getLocalPort)
    finally { socket.close(); accept.join() }
  }

  private def upgrade(host: String, port: Int): Unit = {
    val plain = new Socket()
    plain.connect(new InetSocketAddress("127.0.0.1", port), 5000)
    try { val _ = Tls.buildUpgrade(Some(TlsConfig(TrustSource.Custom(material._2))), host, port)(plain) }
    finally plain.close()
  }

  test("hostname verification accepts a certificate whose SAN matches the connect host") {
    withServer(port => upgrade("localhost", port))
  }

  test("hostname verification rejects a certificate whose SAN does not match the connect host") {
    withServer { port =>
      intercept[TlsError](upgrade("sage-mismatch.invalid", port))
      ()
    }
  }
}

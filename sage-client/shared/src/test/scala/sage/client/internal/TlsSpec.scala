package sage.client.internal

import java.net.{InetAddress, InetSocketAddress, Socket}
import java.nio.file.{Files, Path}
import java.security.KeyStore
import java.security.cert.Certificate
import javax.net.ssl.{KeyManagerFactory, SSLContext, SSLServerSocket, TrustManagerFactory}

import sage.SageException.TlsError
import sage.client.{SageConfig, TlsConfig, TrustSource}
import sage.cluster.Node

class TlsSpec extends munit.FunSuite {

  // Use one self-signed certificate with dns:localhost as its only subject alternative name. The server presents it and the client trusts it;
  // each test changes only the connection host.
  private lazy val material = certMaterial()

  private def certificate: Certificate = material._3

  private def certMaterial(): (SSLContext, SSLContext, Certificate) = {
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
    (server, client, ks.getCertificate("server"))
  }

  private def withServer(body: Int => Unit): Unit = {
    val socket = material._1.getServerSocketFactory
      .createServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))
      .asInstanceOf[SSLServerSocket]
    socket.setSoTimeout(10000)
    val accept = Thread.ofVirtual().start { () =>
      try {
        val s = socket.accept()
        s.getInputStream.read()
        s.close()
      } catch { case _: Throwable => () }
    }
    try body(socket.getLocalPort)
    finally {
      socket.close()
      accept.join()
    }
  }

  private def upgrade(host: String, port: Int): Unit = {
    val plain = new Socket()
    plain.connect(new InetSocketAddress("127.0.0.1", port), 5000)
    try Tls.buildUpgrade(Some(TlsConfig(TrustSource.Custom(material._2))), host, port)(plain): Unit
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

  test("a node's connections share the TLS context built for the node, so connecting after its trust file is gone succeeds") {
    val pem     = Files.createTempFile("sage-tls-trust", ".pem")
    val encoded = java.util.Base64.getMimeEncoder.encodeToString(certificate.getEncoded)
    Files.writeString(pem, s"-----BEGIN CERTIFICATE-----\n$encoded\n-----END CERTIFICATE-----\n")
    withServer { port =>
      val factory   = Client.transports(SageConfig(tls = Some(TlsConfig(TrustSource.Pem(pem)))))(Node("localhost", port))
      Files.delete(pem)
      val transport = factory(_ => (), () => ())
      try transport.start()
      finally transport.close()
    }
  }

  test("unusable trust material fails with TlsError before connecting, even to an unreachable node") {
    val missing = Files.createTempFile("sage-tls-trust", ".pem")
    Files.delete(missing)
    intercept[TlsError](Client.transports(SageConfig(tls = Some(TlsConfig(TrustSource.Pem(missing)))))(Node("localhost", 1)))
  }
}

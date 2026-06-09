package sage.integration.security

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.security.KeyStore
import java.util.Base64

import org.testcontainers.DockerClientFactory

/**
  * A self-signed server certificate generated once at test time, with the Docker host baked into the SAN alongside localhost/127.0.0.1, so
  * hostname verification passes whatever host Testcontainers reports (local daemon, Docker Desktop, or a remote `DOCKER_HOST`). keytool
  * ships with every JDK; the PEM forms are extracted from the resulting PKCS12 in pure Java, so there is no openssl dependency.
  *
  * The cert/key reach the container as bytes over the Docker API (a bind mount would resolve on the daemon host, not the test runner — it
  * fails against a remote daemon). The client trusts the cert from a local file, which is correct: the client runs on the test runner.
  */
object TlsFixture {

  private lazy val material = generate()

  /**
    * A local file holding the server certificate, for the client's PEM trust material.
    */
  def serverCert: Path = material.certFile

  /**
    * The server certificate in PEM, to copy into the container as `--tls-cert-file`.
    */
  def serverCertPem: Array[Byte] = material.certPem

  /**
    * The server private key in PEM, to copy into the container as `--tls-key-file`.
    */
  def serverKeyPem: Array[Byte] = material.keyPem

  final private case class Material(certFile: Path, certPem: Array[Byte], keyPem: Array[Byte])

  private def generate(): Material = {
    val dir       = Files.createTempDirectory("sage-tls")
    val keystore  = dir.resolve("server.p12")
    val storePass = "changeit"
    val keytool   = Path.of(System.getProperty("java.home"), "bin", "keytool").toString

    runOrThrow(
      Seq(
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
        keystore.toString,
        "-storepass",
        storePass,
        "-dname",
        "CN=localhost",
        "-ext",
        s"san=${sanEntries.mkString(",")}"
      )
    )

    val ks = KeyStore.getInstance("PKCS12")
    val in = Files.newInputStream(keystore)
    try ks.load(in, storePass.toCharArray)
    finally in.close()

    val certPem  = pem("CERTIFICATE", ks.getCertificate("server").getEncoded)
    val keyPem   = pem("PRIVATE KEY", ks.getKey("server", storePass.toCharArray).getEncoded)
    val certFile = Files.write(dir.resolve("server.crt"), certPem)
    Material(certFile, certPem, keyPem)
  }

  // getHost() returns the docker host ip address (or localhost); cover it plus the loopback names so the connect host is always in the SAN
  private def sanEntries: Set[String] = {
    val hosts = Set("localhost", "127.0.0.1", DockerClientFactory.instance().dockerHostIpAddress())
    hosts.map(h => if (h.matches("""\d{1,3}(\.\d{1,3}){3}""")) s"ip:$h" else s"dns:$h")
  }

  private def pem(label: String, der: Array[Byte]): Array[Byte] = {
    val body = Base64.getMimeEncoder(64, Array('\n'.toByte)).encodeToString(der)
    s"-----BEGIN $label-----\n$body\n-----END $label-----\n".getBytes(UTF_8)
  }

  private def runOrThrow(command: Seq[String]): Unit = {
    val process = new ProcessBuilder(command*).redirectErrorStream(true).start()
    val output  = new String(process.getInputStream.readAllBytes())
    if (process.waitFor() != 0) throw new IllegalStateException(s"keytool failed: $output")
  }
}

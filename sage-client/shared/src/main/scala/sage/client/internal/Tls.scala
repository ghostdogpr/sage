package sage.client.internal

import java.io.InputStream
import java.net.Socket
import java.nio.file.{Files, Path}
import java.security.{KeyStore, SecureRandom}
import java.security.cert.X509Certificate
import javax.net.ssl.{SSLContext, SSLSocket, TrustManager, TrustManagerFactory, X509TrustManager}

import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

import sage.SageException.TlsError
import sage.client.{TlsConfig, TrustSource}

private[client] object Tls {

  private val plaintext: Socket => Socket = socket => socket

  /**
    * The per-client socket upgrade. The `SSLContext` is built once here, so unusable trust material fails eagerly as a [[TlsError]]; the
    * returned closure layers an `SSLSocket` over an already-connected plain socket and runs the handshake, preserving the plain connect's
    * timeout/abort contract. `host` drives SNI and hostname verification, so it must be the host the socket connected to.
    */
  def buildUpgrade(tls: Option[TlsConfig], host: String, port: Int): Socket => Socket =
    tls match {
      case None         => plaintext
      case Some(config) =>
        val (context, verifyHostname) = contextFor(config.trust)
        val factory                   = context.getSocketFactory
        plain => {
          val ssl = factory.createSocket(plain, host, port, /*autoClose*/ true).asInstanceOf[SSLSocket]
          if (verifyHostname) {
            val params = ssl.getSSLParameters
            params.setEndpointIdentificationAlgorithm("HTTPS")
            ssl.setSSLParameters(params)
          }
          ssl.startHandshake()
          ssl
        }
    }

  private def contextFor(trust: TrustSource): (SSLContext, Boolean) =
    try
      trust match {
        case TrustSource.System              => (SSLContext.getDefault, true)
        case TrustSource.Custom(context)     => (context, true)
        case TrustSource.Insecure            => (trustAllContext(), false)
        case TrustSource.TrustStore(path, p) => (contextOf(trustManagersOfKeyStore(path, p)), true)
        case TrustSource.Pem(path)           => (contextOf(trustManagersOfPem(path)), true)
      }
    catch {
      case e: TlsError => throw e
      case NonFatal(e) => throw TlsError(s"unusable TLS trust material: $e")
    }

  private def contextOf(trustManagers: Array[TrustManager]): SSLContext = {
    val context = SSLContext.getInstance("TLS")
    context.init(null, trustManagers, new SecureRandom)
    context
  }

  private def reading[A](path: Path)(f: InputStream => A): A = {
    val in = Files.newInputStream(path)
    try f(in)
    finally in.close()
  }

  private def trustManagersOfKeyStore(path: Path, password: Option[String]): Array[TrustManager] = {
    val keyStore = KeyStore.getInstance(keyStoreType(path))
    reading(path)(in => keyStore.load(in, password.map(_.toCharArray).orNull))
    factoryFor(keyStore).getTrustManagers
  }

  private def trustManagersOfPem(path: Path): Array[TrustManager] = {
    val factory  = java.security.cert.CertificateFactory.getInstance("X.509")
    val certs    = reading(path)(in => factory.generateCertificates(in).asScala.toVector)
    if (certs.isEmpty) throw TlsError(s"no certificates found in PEM file $path")
    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    keyStore.load(null, null)
    certs.iterator.zipWithIndex.foreach { case (cert, i) => keyStore.setCertificateEntry(s"ca-$i", cert) }
    factoryFor(keyStore).getTrustManagers
  }

  private def factoryFor(keyStore: KeyStore): TrustManagerFactory = {
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(keyStore)
    tmf
  }

  private def keyStoreType(path: Path): String = {
    val name = path.getFileName.toString.toLowerCase
    if (name.endsWith(".p12") || name.endsWith(".pfx")) "PKCS12"
    else if (name.endsWith(".jks")) "JKS"
    else KeyStore.getDefaultType
  }

  private def trustAllContext(): SSLContext =
    contextOf(Array(new X509TrustManager {
      def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
      def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
      def getAcceptedIssuers: Array[X509Certificate]                                = Array.empty
    }))
}

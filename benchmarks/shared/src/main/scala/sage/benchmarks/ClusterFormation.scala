package sage.benchmarks

import java.io.{BufferedReader, InputStreamReader, OutputStreamWriter}
import java.net.Socket
import java.nio.charset.StandardCharsets.UTF_8

/**
  * Forms a single-node cluster on a freshly-started `--cluster-enabled` server: the node claims every slot and announces the
  * testcontainers-mapped host/port so the address it reports in `CLUSTER SLOTS` is reachable from the benchmark (the same trick as the
  * integration `ClusterSuite`).
  */
object ClusterFormation {

  def formSingleNodeCluster(host: String, port: Int): Unit = {
    val socket = new Socket(host, port)
    try {
      val out                            = new OutputStreamWriter(socket.getOutputStream, UTF_8)
      val in                             = new BufferedReader(new InputStreamReader(socket.getInputStream, UTF_8))
      def command(args: String*): String = {
        out.write(args.mkString("", " ", "\r\n"))
        out.flush()
        reply(in)
      }
      def clusterOk: Boolean             = command("CLUSTER", "INFO").contains("cluster_state:ok")

      val _        = command("CONFIG", "SET", "cluster-announce-ip", host)
      val _        = command("CONFIG", "SET", "cluster-announce-port", port.toString)
      if (!clusterOk) { val _ = command("CLUSTER", "ADDSLOTSRANGE", "0", "16383") }
      var attempts = 100
      var ok       = clusterOk
      while (!ok && attempts > 0) {
        Thread.sleep(100)
        attempts -= 1
        ok = clusterOk
      }
      if (!ok) throw new IllegalStateException("single-node cluster did not converge")
    } finally socket.close()
  }

  private def reply(in: BufferedReader): String = {
    val line = in.readLine()
    if (line == null) throw new IllegalStateException("connection closed while forming the cluster")
    else if (line.startsWith("-")) throw new IllegalStateException(s"server error: $line")
    else if (line.startsWith("$")) {
      val length = line.drop(1).toInt
      if (length < 0) ""
      else {
        val payload = new Array[Char](length)
        var read    = 0
        while (read < length) {
          val n = in.read(payload, read, length - read)
          if (n < 0) throw new IllegalStateException("connection closed while forming the cluster")
          read += n
        }
        in.readLine()
        new String(payload)
      }
    } else line
  }
}

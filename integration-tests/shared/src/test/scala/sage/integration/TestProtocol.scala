package sage.integration

import java.util.Locale

import sage.client.RespProtocol

/**
  * The RESP version that the integration tests' clients negotiate. Set the `SAGE_IT_PROTOCOL` environment variable to `auto` (the default),
  * `resp3`, or `resp2` to run the whole suite under that setting, for example `SAGE_IT_PROTOCOL=resp2 sbt itZio`.
  */
object TestProtocol {

  val selected: RespProtocol =
    sys.env.get("SAGE_IT_PROTOCOL").map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty) match {
      case None | Some("auto") => RespProtocol.Auto
      case Some("resp3")       => RespProtocol.Resp3
      case Some("resp2")       => RespProtocol.Resp2
      case Some(other)         => throw new IllegalArgumentException(s"SAGE_IT_PROTOCOL must be auto, resp3, or resp2, but it is '$other'")
    }

  // Auto negotiates RESP3 with every server the shared suites start, so only a forced RESP2 run turns caching off
  val isResp2: Boolean = selected == RespProtocol.Resp2
}

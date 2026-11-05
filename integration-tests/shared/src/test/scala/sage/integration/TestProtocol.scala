package sage.integration

import sage.client.RespProtocol

/**
  * The RESP version that the integration tests' clients negotiate. Set the `SAGE_IT_PROTOCOL` environment variable to `auto` (the default)
  * or `resp2` to run the whole suite under that setting, for example `SAGE_IT_PROTOCOL=resp2 sbt itZio`.
  */
object TestProtocol {

  val selected: RespProtocol =
    sys.env.get("SAGE_IT_PROTOCOL").map(_.trim).filter(_.nonEmpty).fold(RespProtocol.Auto) { name =>
      Vector(RespProtocol.Auto, RespProtocol.Resp2)
        .find(_.toString.equalsIgnoreCase(name))
        .getOrElse(throw new IllegalArgumentException(s"SAGE_IT_PROTOCOL must be auto or resp2, but it is '$name'"))
    }
}

package sage.client

import scala.concurrent.duration.*

final case class SageConfig(
  host: String = "localhost",
  port: Int = 6379,
  connectTimeout: FiniteDuration = 10.seconds
)

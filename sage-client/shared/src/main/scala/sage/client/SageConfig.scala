package sage.client

import scala.concurrent.duration.*

/**
  * Exponential reconnect backoff with full jitter (a random wait in `[0, base]`), spreading the reconnect storm across clients.
  */
final case class BackoffConfig(
  initialDelay: FiniteDuration = 50.millis,
  maxDelay: FiniteDuration = 5.seconds,
  multiplier: Double = 2.0
)

/**
  * Idle-PING liveness check. `pingTimeout` is a death detector, not a latency SLA — size it above the slowest healthy reply.
  */
final case class WatchdogConfig(
  pingInterval: FiniteDuration = 60.seconds,
  pingTimeout: FiniteDuration = 30.seconds,
  enabled: Boolean = true
)

final case class SageConfig(
  host: String = "localhost",
  port: Int = 6379,
  connectTimeout: FiniteDuration = 10.seconds,
  reconnect: BackoffConfig = BackoffConfig(),
  watchdog: WatchdogConfig = WatchdogConfig(),
  closeTimeout: FiniteDuration = 5.seconds
)

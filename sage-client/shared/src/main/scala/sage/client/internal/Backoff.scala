package sage.client.internal

import sage.client.BackoffConfig

private[internal] object Backoff {

  // Reconnects and cluster-redirect retries share this formula: exponential backoff capped at maxDelay, then full jitter in [0, base].
  def jitteredMillis(config: BackoffConfig, attempt: Int, scheduler: Scheduler): Long = {
    val capped = config.maxDelay.toMillis
    val raw    = config.initialDelay.toMillis.toDouble * math.pow(config.multiplier, attempt.toDouble)
    val base   = if (raw.isInfinite || raw >= capped.toDouble) capped else math.max(0L, raw.toLong)
    scheduler.jitterMillis(base + 1)
  }
}

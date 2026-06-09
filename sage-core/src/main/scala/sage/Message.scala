package sage

/**
  * One pub/sub delivery on a channel subscription: the channel it arrived on and the decoded payload.
  */
final case class Message[+V](channel: String, payload: V)

/**
  * One pub/sub delivery on a pattern subscription: the glob pattern that matched, the concrete channel, and the decoded payload.
  */
final case class PatternMessage[+V](pattern: String, channel: String, payload: V)

package sage.internal

// Forces the public-surface completeness check at compile time. Lives in the client module because the
// `sage.*` export aggregator (sage/exports.scala) does, and the check must see those exports.
private[sage] object ApiSurfaceCheck {
  ApiSurface.verifyCommandsExported()
}

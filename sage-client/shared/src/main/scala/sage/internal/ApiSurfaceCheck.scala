package sage.internal

// Runs the public API completeness check at compile time. This belongs in the client module because the
// `sage.*` export aggregator (sage/exports.scala) does, and the check must see those exports.
private[sage] object ApiSurfaceCheck {
  ApiSurface.verifyCommandsExported()
}

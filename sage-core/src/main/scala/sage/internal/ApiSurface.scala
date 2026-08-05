package sage.internal

import scala.quoted.*

// Checks at compile time that package `sage` re-exports every public type from `sage.commands`.
// Without this check, a type omitted from the hand-written aggregator in
// sage-client/shared/.../sage/exports.scala would be missing from `import sage.*`.
private[sage] object ApiSurface {
  inline def verifyCommandsExported(): Unit = ${ verifyCommandsExportedImpl }

  private def verifyCommandsExportedImpl(using Quotes): Expr[Unit] = {
    import quotes.reflect.*

    val commands = Symbol.requiredPackage("sage.commands")
    val sage     = Symbol.requiredPackage("sage")

    // A source-level type may have both a type symbol and a term symbol for its companion. Scala records
    // `private[sage]` on the term as `privateWithin`, so all symbols for a name must be public. Names that
    // contain `$` belong to compiler-generated companions, nested cases, or top-level package holders.
    val publicTypes =
      commands.declarations
        .groupBy(_.name)
        .collect {
          case (name, syms)
              if !name.contains("$") && syms.exists(_.isType) &&
                syms.forall(s => s.privateWithin.isEmpty && !s.flags.is(Flags.Private) && !s.flags.is(Flags.Protected)) =>
            name
        }
        .toList

    // Type aliases are package members. Term forwarders are stored in the compiler-generated
    // `*$package` holder for each source file.
    val exported =
      sage.declarations.filter(_.name.endsWith("$package")).flatMap(_.declarations).map(_.name).toSet

    val missing = publicTypes.filterNot(n => exported.contains(n) || sage.typeMember(n) != Symbol.noSymbol).sorted

    if (missing.nonEmpty)
      report.errorAndAbort(
        s"${missing.size} public type(s) in `sage.commands` are not re-exported from `sage` — add them to " +
          s"the `sage.*` aggregator (sage/exports.scala) so `import sage.*` exposes them:\n  " +
          missing.mkString(", ")
      )

    '{ () }
  }
}

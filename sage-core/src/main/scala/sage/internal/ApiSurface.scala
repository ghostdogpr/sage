package sage.internal

import scala.quoted.*

// Compile-time guard: every user-visible type in `sage.commands` must be re-exported from package `sage`
// (the hand-written aggregator in sage-client/shared/.../sage/exports.scala). Forgetting to add a new
// option/result type there silently shrinks `import sage.*`; this turns that omission into a build error.
private[sage] object ApiSurface {
  inline def verifyCommandsExported(): Unit = ${ verifyCommandsExportedImpl }

  private def verifyCommandsExportedImpl(using Quotes): Expr[Unit] = {
    import quotes.reflect.*

    val commands = Symbol.requiredPackage("sage.commands")
    val sage     = Symbol.requiredPackage("sage")

    // Source-level public types of `sage.commands`. Names are grouped because each type carries both a
    // type symbol and a term (companion/module) symbol; `private[sage]` is recorded as `privateWithin` on
    // the term, so a name is user-visible only when none of its symbols restrict visibility. `$`-mangled
    // names are JVM-flattened companions/nested cases and top-level `*$package` holders — never user types.
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

    // Everything reachable through `import sage.*`: type aliases surface as type members of the package;
    // term forwarders live in the synthetic per-file `*$package` holder objects.
    val exported =
      sage.declarations.filter(_.name.endsWith("$package")).flatMap(_.declarations).map(_.name).toSet

    val missing = publicTypes.filterNot(n => exported.contains(n) || sage.typeMember(n) != Symbol.noSymbol).sorted

    if (missing.nonEmpty)
      report.errorAndAbort(
        s"${missing.size} public type(s) in `sage.commands` are not re-exported from `sage` — add them to " +
          s"sage-client/shared/src/main/scala/sage/exports.scala so `import sage.*` exposes them:\n  " +
          missing.mkString(", ")
      )

    '{ () }
  }
}

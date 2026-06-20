import sbt.VirtualAxis

val scala3Version     = "3.3.8"
val scala3NextVersion = "3.8.3" // Kyo requires Scala 3.8.x (Next)

val munitVersion          = "1.3.3"
val testcontainersVersion = "0.44.1"

// backend effect libraries, declared explicitly so Scala Steward keeps them current
val kyoVersion        = "1.0.0-RC4"
val zioVersion        = "2.1.26"
val catsEffectVersion = "3.7.0"
val fs2Version        = "3.13.0"
val oxVersion         = "1.0.5"

// competitor baselines for the runtime benchmark harness (dev-only, never published) — see benchmarks/README.md
val zioRedisVersion   = "1.2.1"
val redis4catsVersion = "2.0.3"
val lettuceVersion    = "7.6.0.RELEASE"
val rediscalaVersion  = "2.1.0"

inThisBuild(
  List(
    scalaVersion     := scala3Version,
    organization     := "com.github.ghostdogpr",
    homepage         := Some(url("https://github.com/ghostdogpr/sage")),
    licenses         := List(License.Apache2),
    scmInfo          := Some(ScmInfo(url("https://github.com/ghostdogpr/sage/"), "scm:git:git@github.com:ghostdogpr/sage.git")),
    developers       := List(Developer("ghostdogpr", "Pierre Ricadat", "ghostdogpr@gmail.com", url("https://github.com/ghostdogpr"))),
    resolvers += Resolver.sonatypeCentralSnapshots,
    compatKyoVersion := kyoVersion
  )
)

name := "sage"

addCommandAlias(
  "fmt",
  "all scalafmtSbt scalafmt test:scalafmt " +
    "benchmarksZio/scalafmt benchmarksCe/scalafmt benchmarksOx/scalafmt benchmarksKyo3_8_3/scalafmt"
)
addCommandAlias(
  "check",
  "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck " +
    "benchmarksZio/scalafmtCheck benchmarksCe/scalafmtCheck benchmarksOx/scalafmtCheck benchmarksKyo3_8_3/scalafmtCheck"
)

addCommandAlias(
  "testUnit",
  "all core/test clientZio/test clientCe/test clientOx/test clientKyo3_8_3/test " +
    "clientFuture/Test/compile integrationTestsFuture/Test/compile " +
    "benchmarksZio/compile benchmarksCe/compile benchmarksOx/compile benchmarksKyo3_8_3/compile " +
    "examplesZio/Compile/compile examplesCe/Compile/compile examplesOx/Compile/compile " +
    "examplesKyo3_8_3/Compile/compile examplesFuture/Compile/compile"
)
addCommandAlias("itZio", "integrationTestsZio/test")
addCommandAlias("itCe", "integrationTestsCe/test")
addCommandAlias("itOx", "integrationTestsOx/test")
addCommandAlias("itKyo", "integrationTestsKyo3_8_3/test")

lazy val root = project
  .in(file("."))
  .settings(publish / skip := true)
  // benchmarks is intentionally NOT aggregated: it is dev-only/on-demand and pulls JMH + testcontainers + competitor clients, which should
  // not be dragged into ordinary root compile/test/CI. The benchAll alias and benchmarks<Cell>/Jmh/run target its cells directly.
  .aggregate(core.projectRefs ++ client.projectRefs ++ integrationTests.projectRefs ++ examples.projectRefs: _*)

// Pure sans-IO core: RESP3 protocol, command model, codecs. Zero external dependencies.
// Built for both Scala LTS (published) and Scala Next (compile-only, so the kyo client cell can depend on it).
lazy val core = (projectMatrix in file("sage-core"))
  .settings(name := "sage-core")
  .settings(commonSettings)
  .settings(parallelUnitTests)
  .defaultAxes(VirtualAxis.jvm, VirtualAxis.scalaVersionAxis(scala3Version, scala3Version))
  .customRow(
    autoScalaLibrary = true,
    axisValues = Seq(VirtualAxis.jvm, VirtualAxis.scalaVersionAxis(scala3Version, scala3Version)),
    process = identity[Project] _
  )
  .customRow(
    autoScalaLibrary = true,
    axisValues = Seq(VirtualAxis.jvm, VirtualAxis.scalaVersionAxis(scala3NextVersion, scala3NextVersion)),
    process = (p: Project) => p.settings(publish / skip := true)
  )

// Runtime written once against kyo-compat, cross-published per backend. JDK 21+.
// The kyo cell builds with Scala Next; the others stay on LTS.
lazy val client = (projectMatrix in file("sage-client"))
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin)
  .settings(name := "sage-client")
  .settings(commonSettings)
  .settings(parallelUnitTests)
  .settings(
    // compatLibrary emits an implicit Future anchor row; it's a compile-only baseline, never published
    publish / skip   := moduleName.value.endsWith("-future"),
    // surfaces the library version to the CLIENT SETINFO LIB-VER announced at connection setup; version comes from sbt-dynver
    buildInfoKeys    := Seq[BuildInfoKey](version),
    buildInfoPackage := "sage.client",
    // pin the backend effect libs per cell rather than inheriting them transitively from kyo-compat-<backend>
    libraryDependencies ++= {
      val m = moduleName.value
      if (m.endsWith("-zio")) Seq("dev.zio" %% "zio" % zioVersion, "dev.zio" %% "zio-streams" % zioVersion)
      else if (m.endsWith("-ce")) Seq("org.typelevel" %% "cats-effect" % catsEffectVersion, "co.fs2" %% "fs2-core" % fs2Version)
      else if (m.endsWith("-ox")) Seq("com.softwaremill.ox" %% "core" % oxVersion)
      else Seq.empty
    }
  )
  .compatLibrary(KyoLib)(VirtualAxis.jvm)(Seq(scala3NextVersion))
  .compatLibrary(ZioLib, CeLib, OxLib)(VirtualAxis.jvm)(Seq(scala3Version))

// the shared testcontainers suite runs once per backend cell, catching backend-specific lowering bugs against real servers;
// command-behavior (sage.integration.commands), security (sage.integration.security), cluster (sage.integration.cluster), and master-replica
// (sage.integration.masterreplica) suites run on one designated cell only — none of per-command behavior, TLS/auth, cluster routing, nor
// master-replica routing (all in the shared layer) can differ per backend
lazy val integrationTests = (projectMatrix in file("integration-tests"))
  .dependsOn(client, core % "test->test")
  .settings(name := "integration-tests")
  .settings(commonSettings)
  .settings(
    publish / skip                        := true,
    libraryDependencies += "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
    // the Future anchor rows compile but don't boot containers
    Test / testOptions += {
      val isAnchor     = moduleName.value.endsWith("-future")
      val isDesignated = moduleName.value.endsWith("-zio")
      val onceOnly     = Set("sage.integration.commands.", "sage.integration.security.", "sage.integration.cluster.", "sage.integration.masterreplica.")
      Tests.Filter(name => !isAnchor && (isDesignated || !onceOnly.exists(name.startsWith)))
    }
  )
  .compatLibrary(KyoLib)(VirtualAxis.jvm)(Seq(scala3NextVersion))
  .compatLibrary(ZioLib, CeLib, OxLib)(VirtualAxis.jvm)(Seq(scala3Version))

// Runnable, never-published usage examples — one cell per backend so each compiles against its own native artifact (ZIO Task, Cats Effect
// IO, Ox direct style, Kyo). Aggregated by root and compiled in CI via the testUnit alias; the forced future anchor cell carries no example
// of its own and just compiles examples/shared. Run by hand against a local server, e.g. `sbt examplesZio/run` — see examples/README.md.
lazy val examples = (projectMatrix in file("examples"))
  .dependsOn(client)
  .settings(name := "examples")
  .settings(commonSettings)
  .settings(publish / skip := true)
  // never-published runnable samples: no API docs to generate, and doc'ing them only surfaces broken links inside third-party sources
  // (e.g. Cats Effect's IOApp) that an example extends
  .settings(Compile / doc / sources := Seq.empty)
  .compatLibrary(KyoLib)(VirtualAxis.jvm)(Seq(scala3NextVersion))
  .compatLibrary(ZioLib, CeLib, OxLib)(VirtualAxis.jvm)(Seq(scala3Version))

// Runtime end-to-end benchmark harness (JMH), dev-only and never published. One cell per backend (the backends are cross-compiled from the
// same sage.* sources and would collide on one classpath, so they cannot share a module). Competitor baselines are added per cell: zio-redis
// to the zio cell, redis4cats to the ce cell, raw Lettuce (async) to the ox cell as the JVM ceiling. kyo is sage-only. See benchmarks/README.md.
lazy val benchmarks = (projectMatrix in file("benchmarks"))
  .dependsOn(client)
  .enablePlugins(JmhPlugin)
  .settings(name := "benchmarks")
  .settings(commonSettings)
  .settings(
    publish / skip                        := true,
    // matrix cells have a non-existent baseDirectory (e.g. benchmarks/ce/jvm); fork JMH from the repo root so the JVM launches and the
    // -rff benchmarks/results/*.json paths resolve there
    Jmh / run / forkOptions               := (Jmh / run / forkOptions).value.withWorkingDirectory((ThisBuild / baseDirectory).value),
    libraryDependencies += "com.dimafeng" %% "testcontainers-scala-core" % testcontainersVersion,
    libraryDependencies ++= {
      val m = moduleName.value
      if (m.endsWith("-zio")) Seq("dev.zio" %% "zio-redis" % zioRedisVersion)
      else if (m.endsWith("-ce")) Seq("dev.profunktor" %% "redis4cats-effects" % redis4catsVersion)
      else if (m.endsWith("-ox")) Seq("io.lettuce" % "lettuce-core" % lettuceVersion, "io.github.rediscala" %% "rediscala" % rediscalaVersion)
      else Seq.empty
    }
  )
  .compatLibrary(KyoLib)(VirtualAxis.jvm)(Seq(scala3NextVersion))
  .compatLibrary(ZioLib, CeLib, OxLib)(VirtualAxis.jvm)(Seq(scala3Version))

// the runtime benchmark harness: runs every backend cell's JMH suite against its own self-provisioned Redis (the future anchor cell is skipped),
// each writing JMH JSON to benchmarks/results/<cell>.json. benchmarks/merge-results.sh merges them into one all.json covering every client
// (uploadable to https://jmh.morethan.io).
addCommandAlias(
  "benchAll",
  ";benchmarksZio/Jmh/run -rf json -rff benchmarks/results/zio.json " +
    ";benchmarksCe/Jmh/run -rf json -rff benchmarks/results/ce.json " +
    ";benchmarksOx/Jmh/run -rf json -rff benchmarks/results/ox.json " +
    ";benchmarksKyo3_8_3/Jmh/run -rf json -rff benchmarks/results/kyo.json"
)

lazy val commonSettings = Def.settings(
  scalacOptions ++= {
    val base = Seq(
      "-deprecation",
      "-no-indent",
      "-release",
      "21",
      "-Wunused:imports,params,privates,implicits,explicits",
      "-Wvalue-discard"
    )
    if (scalaVersion.value.startsWith("3.8")) base :+ "-Xkind-projector"
    else base ++ Seq("-Xfatal-warnings", "-Ykind-projector", "-Yfuture-lazy-vals")
  },
  libraryDependencies += "org.scalameta" %% "munit" % munitVersion % Test,
  Test / fork                            := true
)

// only for container-free cells: integration suites each boot their own container, so running them in
// parallel would multiply peak load and invite timing races
lazy val parallelUnitTests = Def.settings(Test / testForkedParallel := true)

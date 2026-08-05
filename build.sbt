import _root_.io.getkyo.compat.CompatBackendAxis
import sbt.VirtualAxis

val scala3Version     = "3.3.8"
val scala3NextVersion = "3.8.4"                             // Kyo requires Scala 3.8.x (Next)
val scala3NextSuffix  = scala3NextVersion.replace('.', '_') // Kyo cells embed the Next Scala version in their project id

val munitVersion          = "1.3.4"
val testcontainersVersion = "0.44.1"
val otelVersion           = "1.64.0"
val circeVersion          = "0.14.16" // test-only: verifies ValueCodec.emap with a real JSON library

// backend effect libraries, declared explicitly so Scala Steward keeps them current
val kyoVersion        = "1.0.0-RC6"
val zioVersion        = "2.1.26"
val catsEffectVersion = "3.7.0"
val fs2Version        = "3.13.0"
val oxVersion         = "1.0.6"
val pekkoVersion      = "1.6.0"

// competitor baselines for the runtime benchmark harness (dev-only, never published) — see benchmarks/README.md
val zioRedisVersion   = "1.2.1"
val redis4catsVersion = "2.0.3"
val lettuceVersion    = "7.6.0.RELEASE"
val rediscalaVersion  = "2.1.0"
val jedisVersion      = "6.0.0"

// The Pekko backend uses the Future compatibility cell (`kyo-compat-future`) with Pekko Streams. A separate axis name prevents it from
// being deduplicated with the implicit Future anchor. The explicit coordinates make its rows resolve `kyo-compat-future`; the plugin
// would otherwise inject a nonexistent `kyo-compat-pekko` dependency.
val PekkoLib = CompatBackendAxis.external("pekko", "Pekko", "-pekko", Set("jvm"), "io.getkyo", "kyo-compat-future", kyoVersion)

// Kyo no longer ships a Cats Effect binding, so the `ce` axis binds locally to the vendored `sage-compat-ce` module. The name and
// suffixes match the removed built-in, which keeps cell ids and artifact names unchanged.
val CeLib = CompatBackendAxis.local("ce", "Ce", "-ce", Set("jvm"))

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
    s"benchmarksZio/scalafmt benchmarksCe/scalafmt benchmarksOx/scalafmt benchmarksPekko/scalafmt benchmarksKyo$scala3NextSuffix/scalafmt"
)
addCommandAlias(
  "check",
  "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck " +
    s"benchmarksZio/scalafmtCheck benchmarksCe/scalafmtCheck benchmarksOx/scalafmtCheck benchmarksPekko/scalafmtCheck benchmarksKyo$scala3NextSuffix/scalafmtCheck"
)

addCommandAlias(
  "testUnit",
  s"all core/test opentelemetry/test clientZio/test clientCe/test clientOx/test clientPekko/test clientKyo$scala3NextSuffix/test " +
    "clientFuture/Test/compile integrationTestsFuture/Test/compile integrationTestsPekko/Test/compile " +
    s"benchmarksZio/compile benchmarksCe/compile benchmarksOx/compile benchmarksPekko/compile benchmarksKyo$scala3NextSuffix/compile " +
    "examplesZio/Compile/compile examplesCe/Compile/compile examplesOx/Compile/compile examplesPekko/Compile/compile " +
    s"examplesKyo$scala3NextSuffix/Compile/compile examplesFuture/Compile/compile " +
    "ceConformanceCe/test"
)
addCommandAlias("conformanceCe", "ceConformanceCe/test")
addCommandAlias("itZio", "integrationTestsZio/test")
addCommandAlias("itCe", "integrationTestsCe/test")
addCommandAlias("itOx", "integrationTestsOx/test")
addCommandAlias("itPekko", "integrationTestsPekko/test")
addCommandAlias("itKyo", s"integrationTestsKyo$scala3NextSuffix/test")

addCommandAlias("exampleKyo", s"examplesKyo$scala3NextSuffix/run")

addCommandAlias(
  "docAll",
  s"all core/doc opentelemetry/doc compatCe/doc clientZio/doc clientCe/doc clientOx/doc clientPekko/doc clientKyo$scala3NextSuffix/doc"
)

lazy val root = project
  .in(file("."))
  .settings(publish / skip := true)
  // Exclude benchmarks from root aggregation because their JMH, testcontainers, and competitor-client dependencies are needed only for
  // on-demand benchmark runs. Use benchAll or benchmarks<Cell>/Jmh/run to run them directly.
  .aggregate(
    core.projectRefs ++ client.projectRefs ++ opentelemetry.projectRefs ++ integrationTests.projectRefs ++ examples.projectRefs ++
      Seq[ProjectReference](compatCe): _*
  )

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

// OpenTelemetry tracing. LTS-only: a Scala Next (kyo) app consumes the LTS artifact, as it already does for sage-core.
lazy val opentelemetry = (projectMatrix in file("sage-opentelemetry"))
  .dependsOn(core)
  .settings(name := "sage-opentelemetry")
  .settings(commonSettings)
  .settings(parallelUnitTests)
  .settings(
    libraryDependencies ++= Seq(
      "io.opentelemetry" % "opentelemetry-api"         % otelVersion,
      "io.opentelemetry" % "opentelemetry-sdk"         % otelVersion % Test,
      "io.opentelemetry" % "opentelemetry-sdk-testing" % otelVersion % Test
    )
  )
  .defaultAxes(VirtualAxis.jvm, VirtualAxis.scalaVersionAxis(scala3Version, scala3Version))
  .customRow(
    autoScalaLibrary = true,
    axisValues = Seq(VirtualAxis.jvm, VirtualAxis.scalaVersionAxis(scala3Version, scala3Version)),
    process = identity[Project] _
  )

// The Cats Effect binding of the `kyo.compat` package, vendored because Kyo removed it upstream; `sage-compat-ce/README.md` records the
// provenance. The `ce` cells of every matrix bind to this project through `bindLocally`.
lazy val compatCe = project
  .in(file("sage-compat-ce"))
  .settings(name := "sage-compat-ce")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "co.fs2"        %% "fs2-core"    % fs2Version
    )
  )

// Run the conformance suite bundled inside `kyo-compat-plugin` against the vendored `ce` binding, so that a change to the upstream
// `kyo.compat` contract is reported here as a test failure rather than as a compile error in a downstream module. The implicit Future
// anchor row is unused.
lazy val ceConformance = (projectMatrix in file("sage-compat-ce/.conformance"))
  .settings(publish / skip := true)
  .compatLibrary(CeLib)(VirtualAxis.jvm)(Seq(scala3Version))
  .bindLocally(CeLib, compatCe)
  .compatConformance()

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
    // provides the sbt-dynver version for CLIENT SETINFO LIB-VER during connection setup.
    buildInfoKeys    := Seq[BuildInfoKey](version),
    buildInfoPackage := "sage.client",
    // pin the backend effect libs per cell rather than inheriting them transitively from kyo-compat-<backend>
    libraryDependencies ++= {
      val m = moduleName.value
      if (m.endsWith("-zio")) Seq("dev.zio" %% "zio" % zioVersion, "dev.zio" %% "zio-streams" % zioVersion)
      else if (m.endsWith("-ce")) Seq("org.typelevel" %% "cats-effect" % catsEffectVersion, "co.fs2" %% "fs2-core" % fs2Version)
      else if (m.endsWith("-ox")) Seq("com.softwaremill.ox" %% "core" % oxVersion)
      else if (m.endsWith("-pekko"))
        Seq(
          "org.apache.pekko" %% "pekko-stream"      % pekkoVersion,
          "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion
        )
      else Seq.empty
    }
  )
  .compatLibrary(KyoLib)(VirtualAxis.jvm)(Seq(scala3NextVersion))
  .compatLibrary(ZioLib, CeLib, OxLib, PekkoLib)(VirtualAxis.jvm)(Seq(scala3Version))
  .bindLocally(CeLib, compatCe)

// Run the shared testcontainers suite for every backend to test each backend against real servers. Run command behavior, security, cluster,
// master-replica, and rate-limit suites on one backend because those features use the same shared implementation in every backend.
lazy val integrationTests = (projectMatrix in file("integration-tests"))
  .dependsOn(client, core % "test->test")
  .settings(name := "integration-tests")
  .settings(commonSettings)
  .settings(
    publish / skip                        := true,
    libraryDependencies += "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-parser"  % circeVersion % Test,
      "io.circe" %% "circe-generic" % circeVersion % Test
    ),
    // the Future anchor rows compile but don't boot containers
    Test / testOptions += {
      val isAnchor     = moduleName.value.endsWith("-future")
      val isDesignated = moduleName.value.endsWith("-zio")
      val onceOnly     =
        Set(
          "sage.integration.commands.",
          "sage.integration.security.",
          "sage.integration.cluster.",
          "sage.integration.masterreplica.",
          "sage.integration.ratelimit."
        )
      Tests.Filter(name => !isAnchor && (isDesignated || !onceOnly.exists(name.startsWith)))
    }
  )
  .compatLibrary(KyoLib)(VirtualAxis.jvm)(Seq(scala3NextVersion))
  .compatLibrary(ZioLib, CeLib, OxLib, PekkoLib)(VirtualAxis.jvm)(Seq(scala3Version))
  .bindLocally(CeLib, compatCe)

// Build runnable, unpublished examples for ZIO, Cats Effect, Ox, Kyo, and Pekko in separate cells so each uses its native client artifact. The
// root project compiles them in CI through testUnit. The Future cell compiles examples/shared. Run an example against a local server with a
// command such as `sbt examplesZio/run`. See examples/README.md.
lazy val examples = (projectMatrix in file("examples"))
  .dependsOn(client)
  .settings(name := "examples")
  .settings(commonSettings)
  .settings(publish / skip := true)
  // These runnable samples are not published. Skip API documentation because third-party parent types such as Cats Effect's IOApp contain
  // links that cannot be resolved here.
  .settings(Compile / doc / sources := Seq.empty)
  .compatLibrary(KyoLib)(VirtualAxis.jvm)(Seq(scala3NextVersion))
  .compatLibrary(ZioLib, CeLib, OxLib, PekkoLib)(VirtualAxis.jvm)(Seq(scala3Version))
  .bindLocally(CeLib, compatCe)

// Runtime end-to-end benchmark harness (JMH), used only during development. Each backend has a separate cell. The ZIO cell includes
// zio-redis, the Cats Effect cell includes redis4cats, and the Ox cell includes Lettuce, Rediscala, and Jedis. The Kyo and Pekko cells include
// only Sage. See benchmarks/README.md.
lazy val benchmarks = (projectMatrix in file("benchmarks"))
  .dependsOn(client)
  .enablePlugins(JmhPlugin)
  .settings(name := "benchmarks")
  .settings(commonSettings)
  .settings(
    publish / skip                        := true,
    // Matrix cells have a non-existent baseDirectory (e.g. benchmarks/ce/jvm); fork JMH from the repo root so the JVM launches and the
    // -rff benchmarks/results/*.json paths resolve there
    Jmh / run / forkOptions               := (Jmh / run / forkOptions).value.withWorkingDirectory((ThisBuild / baseDirectory).value),
    libraryDependencies += "com.dimafeng" %% "testcontainers-scala-core" % testcontainersVersion,
    libraryDependencies ++= {
      val m = moduleName.value
      if (m.endsWith("-zio")) Seq("dev.zio" %% "zio-redis" % zioRedisVersion)
      else if (m.endsWith("-ce")) Seq("dev.profunktor" %% "redis4cats-effects" % redis4catsVersion)
      else if (m.endsWith("-ox"))
        Seq(
          "io.lettuce"           % "lettuce-core" % lettuceVersion,
          "io.github.rediscala" %% "rediscala"    % rediscalaVersion,
          "redis.clients"        % "jedis"        % jedisVersion
        )
      else Seq.empty
    }
  )
  .compatLibrary(KyoLib)(VirtualAxis.jvm)(Seq(scala3NextVersion))
  .compatLibrary(ZioLib, CeLib, OxLib, PekkoLib)(VirtualAxis.jvm)(Seq(scala3Version))
  .bindLocally(CeLib, compatCe)

// The runtime benchmark harness: runs every backend cell's JMH suite against its own self-provisioned Redis (the future anchor cell is skipped),
// each writing JMH JSON to benchmarks/results/<cell>.json. benchmarks/merge-results.sh merges them into one all.json covering every client
// (uploadable to https://jmh.morethan.io).
addCommandAlias(
  "benchAll",
  ";benchmarksZio/Jmh/run -rf json -rff benchmarks/results/zio.json " +
    ";benchmarksCe/Jmh/run -rf json -rff benchmarks/results/ce.json " +
    ";benchmarksOx/Jmh/run -rf json -rff benchmarks/results/ox.json " +
    ";benchmarksPekko/Jmh/run -rf json -rff benchmarks/results/pekko.json " +
    s";benchmarksKyo$scala3NextSuffix/Jmh/run -rf json -rff benchmarks/results/kyo.json"
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

// Only for container-free cells: integration suites each boot their own container, so running them in
// parallel would multiply peak load and invite timing races
lazy val parallelUnitTests = Def.settings(Test / testForkedParallel := true)

import sbt.VirtualAxis

val scala3Version     = "3.3.7"
val scala3NextVersion = "3.8.3" // Kyo requires Scala 3.8.x (Next)

val kyoCompatVersion = "1.0.0-RC2+64-9487771b-SNAPSHOT"
val munitVersion     = "1.3.2"

inThisBuild(
  List(
    scalaVersion     := scala3Version,
    organization     := "com.github.ghostdogpr",
    homepage         := Some(url("https://github.com/ghostdogpr/sage")),
    licenses         := List(License.Apache2),
    scmInfo          := Some(ScmInfo(url("https://github.com/ghostdogpr/sage/"), "scm:git:git@github.com:ghostdogpr/sage.git")),
    developers       := List(Developer("ghostdogpr", "Pierre Ricadat", "ghostdogpr@gmail.com", url("https://github.com/ghostdogpr"))),
    resolvers += Resolver.sonatypeCentralSnapshots,
    compatKyoVersion := kyoCompatVersion
  )
)

name := "sage"

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

lazy val root = project
  .in(file("."))
  .settings(publish / skip := true)
  .aggregate(core.projectRefs ++ client.projectRefs: _*)

// Pure sans-IO core: RESP3 protocol, command model, codecs. Zero external dependencies.
// Built for both Scala LTS (published) and Scala Next (compile-only, so the kyo client cell can depend on it).
lazy val core = (projectMatrix in file("sage-core"))
  .settings(name := "sage-core")
  .settings(commonSettings)
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
// The kyo cell builds with Scala Next (like proteus/purelogic); the others stay on LTS.
lazy val client = (projectMatrix in file("sage-client"))
  .dependsOn(core)
  .settings(name := "sage-client")
  .settings(commonSettings)
  .settings(
    // the Scala Next compatLibrary call adds an implicit Future anchor row; only the LTS one is published
    publish / skip := scalaVersion.value != scala3Version && moduleName.value.endsWith("-future")
  )
  .compatLibrary(KyoLib)(VirtualAxis.jvm)(Seq(scala3NextVersion))
  .compatLibrary(ZioLib, CeLib, OxLib)(VirtualAxis.jvm)(Seq(scala3Version))

lazy val commonSettings = Def.settings(
  scalacOptions ++= {
    val base = Seq(
      "-deprecation",
      "-no-indent",
      "-release",
      "21",
      "-Wunused:imports,params,privates,implicits,explicits,nowarn",
      "-Wvalue-discard"
    )
    if (scalaVersion.value.startsWith("3.8")) base :+ "-Xkind-projector"
    else base ++ Seq("-Xfatal-warnings", "-Ykind-projector")
  },
  libraryDependencies += "org.scalameta" %% "munit" % munitVersion % Test,
  Test / fork                            := true
)

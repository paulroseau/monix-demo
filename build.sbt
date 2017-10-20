lazy val buildSettings = Seq(
  organization := "polo",
  scalaVersion := "2.12.3"
)

lazy val noPublishSettings = Seq(
  publish         := { },
  publishLocal    := { },
  publishArtifact := false
)

lazy val noTests = Seq(
  test in test := { }
)

lazy val commonSettings = Seq(
  incOptions := incOptions.value.withLogRecompileOnMacro(false),
  scalacOptions ++= commonScalacOptions,
  libraryDependencies ++= Seq(
    D.simulacrum,
    D.machinist,
    compilerPlugin(D.macroParadise),
    compilerPlugin(D.kindProjector)
  ),
  fork in Test := true,
  parallelExecution in Test := false,
  scalacOptions in (Compile, doc) := (scalacOptions in (Compile, doc)).value.filter(_ != "-Xfatal-warnings")
) ++ warnUnusedImport

lazy val commonScalacOptions = Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:experimental.macros",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-value-discard",
  "-Ypartial-unification",
  "-Xfuture",
  "-Xlog-reflective-calls",
  "-Ywarn-inaccessible",
  "-Ypatmat-exhaust-depth", "20",
  "-Ydelambdafy:method",
  "-Xmax-classfile-name","78"
)

lazy val commonJvmSettings = Seq(
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")
)

lazy val resolverSettings = Seq(
  resolvers ++= Seq("Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases/"),
  resolvers ++= Seq("Paidy Releases" at "https://nexus.test.paidy.io/nexus/content/repositories/releases")
)

lazy val warnUnusedImport = Seq(
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 10)) =>
        Seq()
      case Some((2, n)) if n >= 11 =>
        Seq("-Ywarn-unused-import")
    }
  },
  scalacOptions in (Compile, console) ~= {_.filterNot(Seq("-Xlint", "-Ywarn-unused-import").contains)},
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value
)

lazy val wartRemoverSettings = Seq(
  //wartremoverErrors ++= Warts.unsafe
)

lazy val settings = buildSettings ++ commonSettings ++ resolverSettings ++ wartRemoverSettings

///////////////////////////////////////////////////////////////////////////////////////////////////
// Dependencies
///////////////////////////////////////////////////////////////////////////////////////////////////

lazy val D = new {

  val Versions = new {
    val machinist                = "0.6.1"
    val monix                    = "2.3.0"
    val simulacrum               = "0.10.0"

    // Compiler
    val kindProjector            = "0.9.3"
    val macroParadise            = "2.1.0"
  }

  val machinist                = "org.typelevel"                  %%  "machinist"                            % Versions.machinist
  val monix                    = "io.monix"                       %%  "monix"                                % Versions.monix
  val simulacrum               = "com.github.mpilquist"           %%  "simulacrum"                           % Versions.simulacrum

  // Compiler
  val kindProjector      = "org.spire-math"                       %%  "kind-projector"                       % Versions.kindProjector // cross CrossVersion.full
  val macroParadise      = "org.scalamacros"                      %%  "paradise"                             % Versions.macroParadise cross CrossVersion.full
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Projects
///////////////////////////////////////////////////////////////////////////////////////////////////

lazy val root = Project(
    id = "root",
    base = file(".")
  )
  .settings(moduleName := "root")
  .settings(settings)
  .settings(noPublishSettings)
  .aggregate(core)
  .dependsOn(core)

lazy val core = Project(
    id = "core",
    base = file("core")
  )
  .settings(moduleName := "core")
  .settings(settings)
  .settings(
    libraryDependencies ++= Seq(
      D.monix
    )
  )
  .settings(commonJvmSettings)

///////////////////////////////////////////////////////////////////////////////////////////////////
// Commands
///////////////////////////////////////////////////////////////////////////////////////////////////

addCommandAlias("validate",     ";clean;test")

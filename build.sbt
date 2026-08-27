val Scala212 = "2.12.21"
val Scala213 = "2.13.18"
val Scala3   = "3.3.8"

val zioVersion = "2.1.26"

ThisBuild / organization     := "me.jeffshaw.zio"
ThisBuild / organizationName := "Jeffrey Shaw"
ThisBuild / scalaVersion     := Scala213

ThisBuild / licenses := List(License.Apache2)
ThisBuild / homepage := Some(uri("https://github.com/shawjef3/zio-methods"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    uri("https://github.com/shawjef3/zio-methods"),
    "scm:git@github.com:shawjef3/zio-methods.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "shawjef3",
    name  = "Jeffrey Shaw",
    email = "shawjef3@gmail.com",
    url   = uri("https://github.com/shawjef3")
  )
)

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / pomIncludeRepository := (_ => false)
ThisBuild / publishMavenStyle    := true
ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (version.value.endsWith("-SNAPSHOT")) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}

// Target the same minimum Java version as ZIO (JDK 11).
val javaTarget = Seq(
  javacOptions ++= Seq("--release", "11"),
  scalacOptions += "-release:11"
)

lazy val methods = (project in file("."))
  .settings(javaTarget)
  .settings(
    name               := "methods",
    description        := "Stream combinators built on ZIO, extracted from a ZIO fork rather than merged upstream.",
    crossScalaVersions := Seq(Scala212, Scala213, Scala3),
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-streams"       % zioVersion,
      "dev.zio" %% "zio-test"          % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt"      % zioVersion % Test,
      "dev.zio" %% "zio-test-magnolia" % zioVersion % Test,
      "dev.zio" %% "zio-concurrent"    % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val benchmarks = (project in file("benchmarks"))
  .dependsOn(methods)
  .enablePlugins(JmhPlugin)
  .settings(javaTarget)
  .settings(
    name               := "methods-benchmarks",
    crossScalaVersions := Seq(Scala212, Scala213, Scala3),
    publish / skip     := true,
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-streams" % zioVersion
    )
  )

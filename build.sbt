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

val pruneOrphanedJmhClasses =
  taskKey[Unit]("Delete JMH wrapper classes whose benchmark class no longer exists (sbt-jmh#359 workaround)")

// Target the same minimum Java version as ZIO (JDK 11).
val javaTarget = Seq(
  javacOptions ++= Seq("--release", "11"),
  scalacOptions += "-release:11"
)

lazy val methods = (project in file("."))
  .settings(javaTarget)
  .settings(
    name               := "zstream-methods",
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
    ),
    // Drop orphaned JMH wrapper classes before the generator scans.
    //
    // sbt-jmh 0.4.8 sets `Jmh / classDirectory` to `Compile / classDirectory`
    // and uses that one value for two jobs: the directory the bytecode
    // generator scans for `@Benchmark`, and the directory the generated
    // wrappers compile into. Because the wrappers land among the main classes,
    // Zinc cannot prune them, so deleting a benchmark's source leaves its
    // `jmh_generated/<Name>_*` classes behind. The generator rescans them, tries
    // to load the benchmark class they name, and fails the build with
    // `ClassNotFoundException`; sbt 2's disk cache then restores the orphans
    // after any `clean`, so deleting them by hand does not stick.
    //
    // `Jmh` is `config("jmh") extend Test` with `Defaults.testSettings`, so the
    // two roles share one key and cannot be separated from the output side in
    // this version. Upstream splits them by adding a `jmhBytecodeDirectory` for
    // the scan input (sbt/sbt-jmh#359, not yet released). Until then, delete any
    // wrapper whose benchmark class is gone, just before the scan.
    pruneOrphanedJmhClasses := Def.uncached(Def.task {
      val classes = (Compile / classDirectory).value
      val log     = streams.value.log
      // A wrapper is named `<Benchmark>_<method>_jmhTest.class` or
      // `<Benchmark>_jmhType*.class`; the benchmark itself is `<Benchmark>.class`
      // in the package the wrappers sit under.
      val pkg       = classes / "me" / "jeffshaw" / "zio" / "stream"
      val generated = pkg / "jmh_generated"
      if (generated.exists) {
        val orphans = IO
          .listFiles(generated)
          .filter(_.getName.endsWith(".class"))
          .filterNot(f => (pkg / s"${f.getName.takeWhile(_ != '_')}.class").exists)
        if (orphans.nonEmpty) {
          log.info(s"Removing ${orphans.length} orphaned JMH wrapper class(es) (sbt-jmh#359 workaround)")
          IO.delete(orphans)
        }
      }
    }).value,
    Jmh / JmhPlugin.generateJmhSourcesAndResources := Def
      .uncached(
        (Jmh / JmhPlugin.generateJmhSourcesAndResources)
          .dependsOn(pruneOrphanedJmhClasses)
      )
      .value
  )

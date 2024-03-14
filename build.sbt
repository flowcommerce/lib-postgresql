import sbt.Credentials

name := "lib-postgresql-play28"

organization := "io.flow"

scalaVersion := "2.13.10"
ThisBuild / javacOptions ++= Seq("-source", "17", "-target", "17")

enablePlugins(GitVersioning)
git.useGitDescribe := true

lazy val allScalacOptions = Seq(
  "-feature",
  "-Xfatal-warnings",
  "-unchecked",
  "-Xcheckinit",
  "-Xlint:adapted-args",
  "-Ypatmat-exhaust-depth",
  "100", // Fixes: Exhaustivity analysis reached max recursion depth, not all missing cases are reported.
  "-Wconf:src=generated/.*:silent",
  "-Wconf:src=target/.*:silent" // silence the unused imports errors generated by the Play Routes
)

lazy val root = project
  .in(file("."))
  .settings(
    scalafmtOnCompile := true,
    scalacOptions ++= allScalacOptions ++ Seq("-release", "17"),
    libraryDependencies ++= Seq(
      "org.playframework.anorm" %% "anorm" % "2.7.0",
      "io.flow" %% "lib-test-utils-play28" % "0.2.27" % Test,
      "org.postgresql" % "postgresql" % "42.7.2" % Test
    ),
    resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
    resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases",
    resolvers += "Artifactory" at "https://flow.jfrog.io/flow/libs-release/",
    credentials += Credentials(
      "Artifactory Realm",
      "flow.jfrog.io",
      System.getenv("ARTIFACTORY_USERNAME"),
      System.getenv("ARTIFACTORY_PASSWORD")
    ),
    testOptions += Tests.Argument("-oF"),
    Test / javaOptions ++= Seq(
      "--add-exports=java.base/sun.security.x509=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.ssl=ALL-UNNAMED"
    ),
    Test / javaOptions += "-Dconfig.file=conf/test.conf"
  )

publishTo := {
  val host = "https://flow.jfrog.io/flow"
  if (isSnapshot.value) {
    Some("Artifactory Realm" at s"$host/libs-snapshot-local;build.timestamp=" + new java.util.Date().getTime)
  } else {
    Some("Artifactory Realm" at s"$host/libs-release-local")
  }
}

import sbt.Credentials

name := "lib-postgresql-play26"

organization := "io.flow"

scalaVersion in ThisBuild := "2.12.10"

crossScalaVersions := Seq("2.12.10")

version := "0.1.39"

lazy val root = project
  .in(file("."))
  .settings(
  libraryDependencies ++= Seq(
    "com.typesafe.play" %% "anorm" % "2.5.3",
    "io.flow" %% s"lib-test-utils" % "0.0.72" % Test,
    "org.postgresql" % "postgresql" % "42.2.8" % Test      
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
  javaOptions in Test += "-Dconfig.file=conf/test.conf"
)

publishTo := {
  val host = "https://flow.jfrog.io/flow"
  if (isSnapshot.value) {
    Some("Artifactory Realm" at s"$host/libs-snapshot-local;build.timestamp=" + new java.util.Date().getTime)
  } else {
    Some("Artifactory Realm" at s"$host/libs-release-local")
  }
}

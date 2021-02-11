import sbt.Credentials

name := "lib-postgresql-play28"

organization := "io.flow"

scalaVersion := "2.13.3"

version := "0.1.77"

lazy val root = project
  .in(file("."))
  .settings(
  libraryDependencies ++= Seq(
    "org.playframework.anorm" %% "anorm" % "2.6.9",
    "io.flow" %% "lib-test-utils-play28" % "0.1.25" % Test,
    "org.postgresql" % "postgresql" % "42.2.18" % Test
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

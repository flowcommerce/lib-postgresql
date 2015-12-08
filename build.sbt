import play.PlayImport.PlayKeys._

name := "lib-play-postgresql"

organization := "io.flow"

scalaVersion in ThisBuild := "2.11.7"

crossScalaVersions := Seq("2.11.7")

version := "0.0.2-SNAPSHOT"

lazy val root = project
  .in(file("."))
  .enablePlugins(PlayScala)
  .settings(
    libraryDependencies ++= Seq(
      ws,
      jdbc,
      "com.typesafe.play" %% "anorm" % "2.5.0"
    )
)

publishTo := {
  val host = "https://flow.artifactoryonline.com/flow"
  if (isSnapshot.value) {
    Some("Artifactory Realm" at s"$host/libs-snapshot-local;build.timestamp=" + new java.util.Date().getTime)
  } else {
    Some("Artifactory Realm" at s"$host/libs-release-local")
  }
}

credentials += Credentials(Path.userHome / ".ivy2" / ".artifactory")

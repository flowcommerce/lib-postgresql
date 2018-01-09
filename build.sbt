name := "lib-postgresql"

organization := "io.flow"

scalaVersion in ThisBuild := "2.11.12"

crossScalaVersions := Seq("2.11.12", "2.12.4")

version := "0.0.58"

lazy val root = project
  .in(file("."))
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe.play" %% "anorm" % "2.5.3",
      "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % "test"
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

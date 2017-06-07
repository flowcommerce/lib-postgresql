name := "lib-postgresql"

organization := "io.flow"

scalaVersion in ThisBuild := "2.11.8"

version := "0.0.47"

lazy val root = project
  .in(file("."))
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe.play" %% "anorm" % "2.5.3",
      "org.scalatestplus" %% "play" % "1.4.0" % "test"
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

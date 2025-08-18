// Comment to get more information during initialization
logLevel := Level.Warn

addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.4.1")

addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")

resolvers += "Flow Plugins" at "https://flow.jfrog.io/flow/plugins-release/"

addSbtPlugin("io.flow" % "sbt-flow-linter" % "0.0.67")

addSbtPlugin("com.github.sbt" % "sbt-git" % "2.1.0")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.5")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.3.1")

import Dependencies._
import scala.io.Source
ThisBuild / scalaVersion := "2.12.8"

val lines = Source.fromFile("../pyproject.toml").getLines.toList
val versionString = lines
  .filter(_.startsWith("version"))(0)
  .replace("version = ", "")
  .replace("\"", "")

ThisBuild / version := versionString //"0.2.0-SNAPSHOT"
ThisBuild / organization := "org.streamstate"
ThisBuild / organizationName := "streamstate"
Test / fork := true
Test / parallelExecution := true
val sparkVersion = "3.0.0"
javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx2048M",
  "-XX:MaxPermSize=2048M",
  "-XX:+CMSClassUnloadingEnabled"
)
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true

publishTo := Some(
  "streamstatemaven" at "artifactregistry://us-central1-maven.pkg.dev/streamstatetest/streamstatemaven"
)

lazy val root = (project in file("."))
  .settings(
    name := "streamstate-utils",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_1.0.0" % Test,
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.

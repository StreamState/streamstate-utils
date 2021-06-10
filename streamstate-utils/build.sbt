import Dependencies._

ThisBuild / scalaVersion := "2.12.8"
ThisBuild / version := "0.1.0-SNAPSHOT"
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
lazy val root = (project in file("."))
  .settings(
    name := "streamstate-utils",
    libraryDependencies += scalaTest % Test,
    //libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % Test,
    libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_1.0.0" % Test,
    //"com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.12.0" % Test,
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.

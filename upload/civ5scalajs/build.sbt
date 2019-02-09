import sbt._

// enablePlugins(ScalaJSPlugin, WorkbenchPlugin)

name := "civ5scalajs"
version := "0.1"
scalaVersion := "2.12.7"

libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.5"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"


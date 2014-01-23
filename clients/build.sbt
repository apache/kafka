import sbt._
import Keys._
import AssemblyKeys._

name := "clients"

libraryDependencies ++= Seq(
  "com.novocode"          % "junit-interface" % "0.9" % "test"
)

assemblySettings

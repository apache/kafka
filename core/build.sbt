import sbt._
import Keys._

name := "kafka"

resolvers ++= Seq(
  "SonaType ScalaTest repo" at "https://oss.sonatype.org/content/groups/public/org/scalatest/"
)

libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-compiler" % _ )

libraryDependencies ++= Seq(
  "org.apache.zookeeper"  % "zookeeper"   % "3.3.4",
  "com.github.sgroschupf" % "zkclient"    % "0.1",
  "org.xerial.snappy"     % "snappy-java" % "1.0.4.1",
  "org.easymock"          % "easymock"    % "3.0" % "test",
  "junit"                 % "junit"       % "4.1" % "test"
)

libraryDependencies <<= (scalaVersion, libraryDependencies) { (sv, deps) =>
  deps :+ (sv match {
    case "2.8.0" => "org.scalatest" %  "scalatest" % "1.2" % "test"
    case _       => "org.scalatest" %% "scalatest" % "1.8" % "test"
  })
}



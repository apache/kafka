/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import Keys._
import Process._

import scala.xml.{Node, Elem}
import scala.xml.transform.{RewriteRule, RuleTransformer}

object KafkaBuild extends Build {
  val buildNumber = SettingKey[String]("build-number", "Build number defaults to $BUILD_NUMBER environment variable")
  val releaseName = SettingKey[String]("release-name", "the full name of this release")
  val commonSettings = Seq(
    organization := "org.apache",
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-g:none"),
    crossScalaVersions := Seq("2.8.0","2.8.2", "2.9.1", "2.9.2"),
    scalaVersion := "2.8.0",
    version := "0.8.0-SNAPSHOT",
    buildNumber := System.getProperty("build.number", ""),
    version <<= (buildNumber, version)  { (build, version)  => if (build == "") version else version + "+" + build},
    releaseName <<= (name, version, scalaVersion) {(name, version, scalaVersion) => name + "_" + scalaVersion + "-" + version},
    javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.5"),
    parallelExecution in Test := false, // Prevent tests from overrunning each other
    libraryDependencies ++= Seq(
      "log4j"                 % "log4j"        % "1.2.15",
      "net.sf.jopt-simple"    % "jopt-simple"  % "3.2",
      "org.slf4j"             % "slf4j-simple" % "1.6.4"
    ),
    // The issue is going from log4j 1.2.14 to 1.2.15, the developers added some features which required
    // some dependencies on various sun and javax packages.
    ivyXML := <dependencies>
        <exclude module="javax"/>
        <exclude module="jmxri"/>
        <exclude module="jmxtools"/>
        <exclude module="mail"/>
        <exclude module="jms"/>
        <dependency org="org.apache.zookeeper" name="zookeeper" rev="3.3.4">
          <exclude org="log4j" module="log4j"/>
          <exclude org="jline" module="jline"/>
        </dependency>
      </dependencies>
  )

  val hadoopSettings = Seq(
    javacOptions ++= Seq("-Xlint:deprecation"),
    libraryDependencies ++= Seq(
      "org.apache.avro"      % "avro"               % "1.4.0",
      "org.apache.pig"       % "pig"                % "0.8.0",
      "commons-logging"      % "commons-logging"    % "1.0.4",
      "org.codehaus.jackson" % "jackson-core-asl"   % "1.5.5",
      "org.codehaus.jackson" % "jackson-mapper-asl" % "1.5.5",
      "org.apache.hadoop"    % "hadoop-core"        % "0.20.2"
    ),
    ivyXML := 
       <dependencies>
         <exclude module="netty"/>
         <exclude module="javax"/>
         <exclude module="jmxri"/>
         <exclude module="jmxtools"/>
         <exclude module="mail"/>
         <exclude module="jms"/>
         <dependency org="org.apache.hadoop" name="hadoop-core" rev="0.20.2">
           <exclude org="junit" module="junit"/>
         </dependency>
         <dependency org="org.apache.pig" name="pig" rev="0.8.0">
           <exclude org="junit" module="junit"/>
         </dependency>
       </dependencies>
  )


  val runRat = TaskKey[Unit]("run-rat-task", "Runs Apache rat on Kafka")
  val runRatTask = runRat := {
    "bin/run-rat.sh" !
  }

  val release = TaskKey[Unit]("release", "Creates a deployable release directory file with dependencies, config, and scripts.")
  val releaseTask = release <<= ( packageBin in (core, Compile), dependencyClasspath in (core, Runtime), exportedProducts in Compile,
    target, releaseName in core ) map { (packageBin, deps, products, target, releaseName) =>
      val jarFiles = deps.files.filter(f => !products.files.contains(f) && f.getName.endsWith(".jar"))
      val destination = target / "RELEASE" / releaseName
      IO.copyFile(packageBin, destination / packageBin.getName)
      IO.copy(jarFiles.map { f => (f, destination / "libs" / f.getName) })
      IO.copyDirectory(file("config"), destination / "config")
      IO.copyDirectory(file("bin"), destination / "bin")
      for {file <- (destination / "bin").listFiles} { file.setExecutable(true, true) }
  }

  val releaseZip = TaskKey[Unit]("release-zip", "Creates a deployable zip file with dependencies, config, and scripts.")
  val releaseZipTask = releaseZip <<= (release, target, releaseName in core) map { (release, target, releaseName) => 
    val zipPath = target / "RELEASE" / "%s.zip".format(releaseName)
    IO.delete(zipPath)
    IO.zip((target/"RELEASE" ** releaseName ***) x relativeTo(target/"RELEASE"), zipPath)
  }

  val releaseTar = TaskKey[Unit]("release-tar", "Creates a deployable tar.gz file with dependencies, config, and scripts.")
  val releaseTarTask = releaseTar <<= ( release, target, releaseName in core) map { (release, target, releaseName) =>
    Process(Seq("tar", "czf", "%s.tar.gz".format(releaseName), releaseName), target / "RELEASE").! match {
        case 0 => ()
        case n => sys.error("Failed to run native tar application!")
      }
  }

  lazy val kafka    = Project(id = "Kafka", base = file(".")).aggregate(core, examples, contrib, perf).settings((commonSettings ++
    runRatTask ++ releaseTask ++ releaseZipTask ++ releaseTarTask): _*)
  lazy val core     = Project(id = "core", base = file("core")).settings(commonSettings: _*)
  lazy val examples = Project(id = "java-examples", base = file("examples")).settings(commonSettings :_*) dependsOn (core)
  lazy val perf     = Project(id = "perf", base = file("perf")).settings((Seq(name := "kafka-perf") ++ commonSettings):_*) dependsOn (core)

  lazy val contrib        = Project(id = "contrib", base = file("contrib")).aggregate(hadoopProducer, hadoopConsumer).settings(commonSettings :_*)
  lazy val hadoopProducer = Project(id = "hadoop-producer", base = file("contrib/hadoop-producer")).settings(hadoopSettings ++ commonSettings: _*) dependsOn (core)
  lazy val hadoopConsumer = Project(id = "hadoop-consumer", base = file("contrib/hadoop-consumer")).settings(hadoopSettings ++ commonSettings: _*) dependsOn (core)

}

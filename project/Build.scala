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
import java.io.File

import scala.xml.{Node, Elem}
import scala.xml.transform.{RewriteRule, RuleTransformer}

object KafkaBuild extends Build {
  val commonSettings = Seq(
    version := "0.8-SNAPSHOT",
    organization := "org.apache",
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-g:none"),
    crossScalaVersions := Seq("2.8.0","2.8.2", "2.9.1", "2.9.2"),
    scalaVersion := "2.8.0",
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

  val coreSettings = Seq(
    pomPostProcess := { (pom: Node) => MetricsDepAdder(ZkClientDepAdder(pom)) }
  )

  val runRat = TaskKey[Unit]("run-rat-task", "Runs Apache rat on Kafka")
  val runRatTask = runRat := {
    "bin/run-rat.sh" !
  }

  lazy val kafka    = Project(id = "Kafka", base = file(".")).aggregate(core, examples, contrib, perf).settings((commonSettings ++ runRatTask): _*)
  lazy val core     = Project(id = "core", base = file("core")).settings(commonSettings: _*).settings(coreSettings: _*)
  lazy val examples = Project(id = "java-examples", base = file("examples")).settings(commonSettings :_*) dependsOn (core)
  lazy val perf     = Project(id = "perf", base = file("perf")).settings((Seq(name := "kafka-perf") ++ commonSettings):_*) dependsOn (core)

  lazy val contrib        = Project(id = "contrib", base = file("contrib")).aggregate(hadoopProducer, hadoopConsumer).settings(commonSettings :_*)
  lazy val hadoopProducer = Project(id = "hadoop-producer", base = file("contrib/hadoop-producer")).settings(hadoopSettings ++ commonSettings: _*) dependsOn (core)
  lazy val hadoopConsumer = Project(id = "hadoop-consumer", base = file("contrib/hadoop-consumer")).settings(hadoopSettings ++ commonSettings: _*) dependsOn (core)


  // POM Tweaking for core:
  def zkClientDep =
    <dependency>
      <groupId>zkclient</groupId>
      <artifactId>zkclient</artifactId>
      <version>20120522</version>
      <scope>compile</scope>
    </dependency>

  def metricsDeps =
    <dependencies>
      <dependency>
        <groupId>com.yammer.metrics</groupId>
        <artifactId>metrics-core</artifactId>
        <version>3.0.0-c0c8be71</version>
        <scope>compile</scope>
      </dependency>
      <dependency>
        <groupId>com.yammer.metrics</groupId>
        <artifactId>metrics-annotations</artifactId>
        <version>3.0.0-c0c8be71</version>
        <scope>compile</scope>
      </dependency>
    </dependencies>

  object ZkClientDepAdder extends RuleTransformer(new RewriteRule() {
    override def transform(node: Node): Seq[Node] = node match {
      case Elem(prefix, "dependencies", attribs, scope, deps @ _*) => {
        Elem(prefix, "dependencies", attribs, scope, deps ++ zkClientDep:_*)
      }
      case other => other
    }
  })

  object MetricsDepAdder extends RuleTransformer(new RewriteRule() {
    override def transform(node: Node): Seq[Node] = node match {
      case Elem(prefix, "dependencies", attribs, scope, deps @ _*) => {
        Elem(prefix, "dependencies", attribs, scope, deps ++ metricsDeps:_*)
      }
      case other => other
    }
  })

}

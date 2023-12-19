/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.docker

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

class KafkaDockerWrapperTest {

  @Test
  def testGetToolsLog4jConfigsFromEnv(): Unit = {
    val envVars = Map("KAFKA_TOOLS_LOG4J_LOGLEVEL" -> "TRACE")
    val expected = "\n" + "log4j.rootLogger=TRACE, stderr"
    val actual = KafkaDockerWrapper.getToolsLog4jConfigsFromEnv(envVars)
    assertEquals(expected, actual)
  }

  @Test
  def testGetToolsLog4jConfigsFromEnvInvalidEnvVariable(): Unit = {
    val envVars = Map("SOME_VARIABLE" -> "Some Value")
    val expected = ""
    val actual = KafkaDockerWrapper.getToolsLog4jConfigsFromEnv(envVars)
    assertEquals(expected, actual)
  }

  @Test
  def testPrepareToolsLog4jConfigs(): Unit = {
    val (defaultConfigsDir, mountedConfigsDir, finalConfigsDir) = createDirs()

    val envVars = Map("KAFKA_TOOLS_LOG4J_LOGLEVEL" -> "TRACE")

    Files.write(Paths.get(defaultConfigsDir.toString + "/tools-log4j.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(Paths.get(mountedConfigsDir.toString + "/tools-log4j.properties"), "mounted.config=mounted value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(Paths.get(finalConfigsDir.toString + "/tools-log4j.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareToolsLog4jConfigs(defaultConfigsDir.toString, mountedConfigsDir.toString, finalConfigsDir.toString, envVars)

    val source = scala.io.Source.fromFile(finalConfigsDir.toString + "/tools-log4j.properties")
    val actual = try source.mkString finally source.close()
    val expected = "mounted.config=mounted value" + "\n" + "log4j.rootLogger=TRACE, stderr"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareToolsLog4jConfigsWithoutMountedFile(): Unit = {
    val (defaultConfigsDir, mountedConfigsDir, finalConfigsDir) = createDirs()

    val envVars = Map("KAFKA_TOOLS_LOG4J_LOGLEVEL" -> "TRACE")

    Files.write(Paths.get(defaultConfigsDir.toString + "/tools-log4j.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(Paths.get(finalConfigsDir.toString + "/tools-log4j.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareToolsLog4jConfigs(defaultConfigsDir.toString, mountedConfigsDir.toString, finalConfigsDir.toString, envVars)

    val source = scala.io.Source.fromFile(finalConfigsDir.toString + "/tools-log4j.properties")
    val actual = try source.mkString finally source.close()
    val expected = "default.config=default value" + "\n" + "log4j.rootLogger=TRACE, stderr"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareToolsLog4jConfigsWithoutEnvVariable(): Unit = {
    val (defaultConfigsDir, mountedConfigsDir, finalConfigsDir) = createDirs()

    val envVars = Map.empty[String, String]

    Files.write(Paths.get(defaultConfigsDir.toString + "/tools-log4j.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(Paths.get(mountedConfigsDir.toString + "/tools-log4j.properties"), "mounted.config=mounted value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(Paths.get(finalConfigsDir.toString + "/tools-log4j.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareToolsLog4jConfigs(defaultConfigsDir.toString, mountedConfigsDir.toString, finalConfigsDir.toString, envVars)

    val source = scala.io.Source.fromFile(finalConfigsDir.toString + "/tools-log4j.properties")
    val actual = try source.mkString finally source.close()
    val expected = "mounted.config=mounted value"

    assertEquals(expected, actual)
  }

  @Test
  def testGetServerConfigsFromEnv(): Unit = {
    val envVars = Map("KAFKA_TOOLS_LOG4J_LOGLEVEL" -> "TRACE",
                      "KAFKA_VALID_PROPERTY" -> "Value",
                      "SOME_VARIABLE" -> "Some Value",
                      "KAFKA_VALID___PROPERTY__ALL_CASES" -> "All Cases Value")
    val expected = List("valid.property=Value", "valid-property_all.cases=All Cases Value")
    val actual = KafkaDockerWrapper.getServerConfigsFromEnv(envVars)
    assertEquals(expected, actual)
  }

  @Test
  def testPrepareServerConfigs(): Unit = {
    val (defaultConfigsDir, mountedConfigsDir, finalConfigsDir) = createDirs()

    val envVars = Map("KAFKA_ENV_CONFIG" -> "env value")

    Files.write(Paths.get(defaultConfigsDir.toString + "/server.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(Paths.get(mountedConfigsDir.toString + "/server.properties"), "mounted.config=mounted value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(Paths.get(finalConfigsDir.toString + "/server.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareServerConfigs(defaultConfigsDir.toString, mountedConfigsDir.toString, finalConfigsDir.toString, envVars)

    val source = scala.io.Source.fromFile(finalConfigsDir.toString + "/server.properties")
    val actual = try source.mkString finally source.close()
    val expected = "mounted.config=mounted value" + "\n" + "env.config=env value"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareServerConfigsWithoutMountedFile(): Unit = {
    val (defaultConfigsDir, mountedConfigsDir, finalConfigsDir) = createDirs()

    val envVars = Map("KAFKA_ENV_CONFIG" -> "env value")

    Files.write(Paths.get(defaultConfigsDir.toString + "/server.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(Paths.get(finalConfigsDir.toString + "/server.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareServerConfigs(defaultConfigsDir.toString, mountedConfigsDir.toString, finalConfigsDir.toString, envVars)

    val source = scala.io.Source.fromFile(finalConfigsDir.toString + "/server.properties")
    val actual = try source.mkString finally source.close()
    val expected = "\n" + "env.config=env value"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareServerConfigsWithoutEnvVariables(): Unit = {
    val (defaultConfigsDir, mountedConfigsDir, finalConfigsDir) = createDirs()

    val envVars = Map.empty[String, String]

    Files.write(Paths.get(defaultConfigsDir.toString + "/server.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(Paths.get(mountedConfigsDir.toString + "/server.properties"), "mounted.config=mounted value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(Paths.get(finalConfigsDir.toString + "/server.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareServerConfigs(defaultConfigsDir.toString, mountedConfigsDir.toString, finalConfigsDir.toString, envVars)

    val source = scala.io.Source.fromFile(finalConfigsDir.toString + "/server.properties")
    val actual = try source.mkString finally source.close()
    val expected = "mounted.config=mounted value"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareServerConfigsWithoutUserInput(): Unit = {
    val (defaultConfigsDir, mountedConfigsDir, finalConfigsDir) = createDirs()

    val envVars = Map.empty[String, String]

    Files.write(Paths.get(defaultConfigsDir.toString + "/server.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(Paths.get(finalConfigsDir.toString + "/server.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareServerConfigs(defaultConfigsDir.toString, mountedConfigsDir.toString, finalConfigsDir.toString, envVars)

    val source = scala.io.Source.fromFile(finalConfigsDir.toString + "/server.properties")
    val actual = try source.mkString finally source.close()
    val expected = "default.config=default value"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareServerConfigsWithEmptyMountedFile(): Unit = {
    val (defaultConfigsDir, mountedConfigsDir, finalConfigsDir) = createDirs()

    val envVars = Map.empty[String, String]

    Files.write(Paths.get(defaultConfigsDir.toString + "/server.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(Paths.get(mountedConfigsDir.toString + "/server.properties"), " \n \n ".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(Paths.get(finalConfigsDir.toString + "/server.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareServerConfigs(defaultConfigsDir.toString, mountedConfigsDir.toString, finalConfigsDir.toString, envVars)

    val source = scala.io.Source.fromFile(finalConfigsDir.toString + "/server.properties")
    val actual = try source.mkString finally source.close()
    val expected = "default.config=default value"

    assertEquals(expected, actual)
  }

  private def createDirs(): (Path, Path, Path) = {
    val defaultConfigsDir = Files.createTempDirectory("tmp")
    val mountedConfigsDir = Files.createTempDirectory("tmp")
    val finalConfigsDir = Files.createTempDirectory("tmp")

    defaultConfigsDir.toFile.deleteOnExit()
    mountedConfigsDir.toFile.deleteOnExit()
    finalConfigsDir.toFile.deleteOnExit()

    (defaultConfigsDir, mountedConfigsDir, finalConfigsDir)
  }
}

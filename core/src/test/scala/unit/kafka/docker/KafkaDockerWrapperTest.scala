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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class KafkaDockerWrapperTest {
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
    val (defaultConfigsPath, mountedConfigsPath, finalConfigsPath) = createDirs()

    val envVars = Map("KAFKA_ENV_CONFIG" -> "env value")

    Files.write(defaultConfigsPath.resolve("server.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(mountedConfigsPath.resolve("server.properties"), "mounted.config=mounted value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(finalConfigsPath.resolve("server.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareServerConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)

    val source = scala.io.Source.fromFile(finalConfigsPath.toString + "/server.properties")
    val actual = try source.mkString finally source.close()
    val expected = "mounted.config=mounted value" + "\n" + "env.config=env value"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareServerConfigsWithoutMountedFile(): Unit = {
    val (defaultConfigsPath, mountedConfigsPath, finalConfigsPath) = createDirs()

    val envVars = Map("KAFKA_ENV_CONFIG" -> "env value")

    Files.write(defaultConfigsPath.resolve("server.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(finalConfigsPath.resolve("server.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareServerConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)

    val source = scala.io.Source.fromFile(finalConfigsPath.toString + "/server.properties")
    val actual = try source.mkString finally source.close()
    val expected = "\n" + "env.config=env value"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareServerConfigsWithoutEnvVariables(): Unit = {
    val (defaultConfigsPath, mountedConfigsPath, finalConfigsPath) = createDirs()

    val envVars = Map.empty[String, String]

    Files.write(defaultConfigsPath.resolve("server.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(mountedConfigsPath.resolve("server.properties"), "mounted.config=mounted value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(finalConfigsPath.resolve("server.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareServerConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)

    val source = scala.io.Source.fromFile(finalConfigsPath.toString + "/server.properties")
    val actual = try source.mkString finally source.close()
    val expected = "mounted.config=mounted value"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareServerConfigsWithoutUserInput(): Unit = {
    val (defaultConfigsPath, mountedConfigsPath, finalConfigsPath) = createDirs()

    val envVars = Map.empty[String, String]

    Files.write(defaultConfigsPath.resolve("server.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(finalConfigsPath.resolve("server.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareServerConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)

    val source = scala.io.Source.fromFile(finalConfigsPath.toString + "/server.properties")
    val actual = try source.mkString finally source.close()
    val expected = "default.config=default value"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareServerConfigsWithEmptyMountedFile(): Unit = {
    val (defaultConfigsPath, mountedConfigsPath, finalConfigsPath) = createDirs()

    val envVars = Map.empty[String, String]

    Files.write(defaultConfigsPath.resolve("server.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(mountedConfigsPath.resolve("server.properties"), " \n \n ".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(finalConfigsPath.resolve("server.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareServerConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)

    val source = scala.io.Source.fromFile(finalConfigsPath.toString + "/server.properties")
    val actual = try source.mkString finally source.close()
    val expected = "default.config=default value"

    assertEquals(expected, actual)
  }

  @Test
  def testGetLog4jConfigsFromEnv(): Unit = {
    val envVars = Map(
      "KAFKA_LOG4J_LOGGERS" -> "kafka=INFO,kafka.network.RequestChannel$=WARN,kafka.producer.async.DefaultEventHandler=DEBUG,",
      "KAFKA_LOG4J_ROOT_LOGLEVEL" -> "ERROR",
      "SOME_VARIABLE" -> "Some Value"
    )
    val expected = "\n" + "log4j.rootLogger=ERROR, stdout" + "\n" +
      "log4j.logger.kafka=INFO" + "\n" +
      "log4j.logger.kafka.network.RequestChannel$=WARN" + "\n" +
      "log4j.logger.kafka.producer.async.DefaultEventHandler=DEBUG"

    val actual = KafkaDockerWrapper.getLog4jConfigsFromEnv(envVars)
    assertEquals(expected, actual)
  }

  @Test
  def testGetLog4jConfigsFromEnvInvalidEnvVariable(): Unit = {
    val envVars = Map("SOME_VARIABLE" -> "Some Value")
    val expected = ""

    val actual = KafkaDockerWrapper.getLog4jConfigsFromEnv(envVars)
    assertEquals(expected, actual)
  }

  @Test
  def testGetLog4jConfigsFromEnvWithEmptyEnvVariable(): Unit = {
    val envVars = Map("SOME_VARIABLE" -> "Some Value", "KAFKA_LOG4J_LOGGERS" -> "", "KAFKA_LOG4J_ROOT_LOGLEVEL" -> "")
    val expected = ""

    val actual = KafkaDockerWrapper.getLog4jConfigsFromEnv(envVars)
    assertEquals(expected, actual)
  }

  @Test
  def testPrepareLog4jConfigs(): Unit = {
    val (defaultConfigsPath, mountedConfigsPath, finalConfigsPath) = createDirs()

    val envVars = Map(
      "KAFKA_LOG4J_LOGGERS" -> "kafka=INFO,kafka.network.RequestChannel$=WARN,kafka.producer.async.DefaultEventHandler=DEBUG,",
      "KAFKA_LOG4J_ROOT_LOGLEVEL" -> "ERROR",
      "SOME_VARIABLE" -> "Some Value"
    )

    Files.write(defaultConfigsPath.resolve("log4j.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(mountedConfigsPath.resolve("log4j.properties"), "mounted.config=mounted value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(finalConfigsPath.resolve("log4j.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareLog4jConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)

    val source = scala.io.Source.fromFile(finalConfigsPath.toString + "/log4j.properties")
    val actual = try source.mkString finally source.close()
    val expected = "mounted.config=mounted value" + "\n" + "log4j.rootLogger=ERROR, stdout" + "\n" +
      "log4j.logger.kafka=INFO" + "\n" +
      "log4j.logger.kafka.network.RequestChannel$=WARN" + "\n" +
      "log4j.logger.kafka.producer.async.DefaultEventHandler=DEBUG"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareLog4jConfigsWithoutMountedFile(): Unit = {
    val (defaultConfigsPath, mountedConfigsPath, finalConfigsPath) = createDirs()

    val envVars = Map(
      "KAFKA_LOG4J_LOGGERS" -> "kafka=INFO,kafka.network.RequestChannel$=WARN,kafka.producer.async.DefaultEventHandler=DEBUG,",
      "KAFKA_LOG4J_ROOT_LOGLEVEL" -> "ERROR",
      "SOME_VARIABLE" -> "Some Value"
    )

    Files.write(defaultConfigsPath.resolve("log4j.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(finalConfigsPath.resolve("log4j.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareLog4jConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)

    val source = scala.io.Source.fromFile(finalConfigsPath.toString + "/log4j.properties")
    val actual = try source.mkString finally source.close()
    val expected = "default.config=default value" + "\n" + "log4j.rootLogger=ERROR, stdout" + "\n" +
      "log4j.logger.kafka=INFO" + "\n" +
      "log4j.logger.kafka.network.RequestChannel$=WARN" + "\n" +
      "log4j.logger.kafka.producer.async.DefaultEventHandler=DEBUG"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareLog4jConfigsWithoutEnvVariables(): Unit = {
    val (defaultConfigsPath, mountedConfigsPath, finalConfigsPath) = createDirs()

    val envVars = Map.empty[String, String]

    Files.write(defaultConfigsPath.resolve("log4j.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(mountedConfigsPath.resolve("log4j.properties"), "mounted.config=mounted value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(finalConfigsPath.resolve("log4j.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareLog4jConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)

    val source = scala.io.Source.fromFile(finalConfigsPath.toString + "/log4j.properties")
    val actual = try source.mkString finally source.close()
    val expected = "mounted.config=mounted value"

    assertEquals(expected, actual)
  }

  @Test
  def testGetToolsLog4jConfigsFromEnv(): Unit = {
    val envVars = Map("KAFKA_TOOLS_LOG4J_LOGLEVEL" -> "TRACE", "SOME_VARIABLE" -> "Some Value")
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
  def testGetToolsLog4jConfigsFromEnvWithEmptyEnvVariable(): Unit = {
    val envVars = Map("SOME_VARIABLE" -> "Some Value", "KAFKA_TOOLS_LOG4J_LOGLEVEL" -> "")
    val expected = ""
    val actual = KafkaDockerWrapper.getToolsLog4jConfigsFromEnv(envVars)
    assertEquals(expected, actual)
  }

  @Test
  def testPrepareToolsLog4jConfigs(): Unit = {
    val (defaultConfigsPath, mountedConfigsPath, finalConfigsPath) = createDirs()

    val envVars = Map("KAFKA_TOOLS_LOG4J_LOGLEVEL" -> "TRACE")

    Files.write(defaultConfigsPath.resolve("tools-log4j.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(mountedConfigsPath.resolve("tools-log4j.properties"), "mounted.config=mounted value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(finalConfigsPath.resolve("tools-log4j.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareToolsLog4jConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)

    val source = scala.io.Source.fromFile(finalConfigsPath.toString + "/tools-log4j.properties")
    val actual = try source.mkString finally source.close()
    val expected = "mounted.config=mounted value" + "\n" + "log4j.rootLogger=TRACE, stderr"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareToolsLog4jConfigsWithoutMountedFile(): Unit = {
    val (defaultConfigsPath, mountedConfigsPath, finalConfigsPath) = createDirs()

    val envVars = Map("KAFKA_TOOLS_LOG4J_LOGLEVEL" -> "TRACE")

    Files.write(defaultConfigsPath.resolve("tools-log4j.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(finalConfigsPath.resolve("tools-log4j.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareToolsLog4jConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)

    val source = scala.io.Source.fromFile(finalConfigsPath.toString + "/tools-log4j.properties")
    val actual = try source.mkString finally source.close()
    val expected = "default.config=default value" + "\n" + "log4j.rootLogger=TRACE, stderr"

    assertEquals(expected, actual)
  }

  @Test
  def testPrepareToolsLog4jConfigsWithoutEnvVariable(): Unit = {
    val (defaultConfigsPath, mountedConfigsPath, finalConfigsPath) = createDirs()

    val envVars = Map.empty[String, String]

    Files.write(defaultConfigsPath.resolve("tools-log4j.properties"), "default.config=default value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(mountedConfigsPath.resolve("tools-log4j.properties"), "mounted.config=mounted value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    Files.write(finalConfigsPath.resolve("tools-log4j.properties"), "existing.config=existing value".getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()

    KafkaDockerWrapper.prepareToolsLog4jConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)

    val source = scala.io.Source.fromFile(finalConfigsPath.toString + "/tools-log4j.properties")
    val actual = try source.mkString finally source.close()
    val expected = "mounted.config=mounted value"

    assertEquals(expected, actual)
  }

  private def createDirs(): (Path, Path, Path) = {
    val defaultConfigsPath = Files.createTempDirectory("tmp")
    val mountedConfigsPath = Files.createTempDirectory("tmp")
    val finalConfigsPath = Files.createTempDirectory("tmp")

    defaultConfigsPath.toFile.deleteOnExit()
    mountedConfigsPath.toFile.deleteOnExit()
    finalConfigsPath.toFile.deleteOnExit()

    (defaultConfigsPath, mountedConfigsPath, finalConfigsPath)
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.kafka.admin

import kafka.api.IntegrationTestHarness
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.config.provider.FileConfigProvider
import org.apache.kafka.common.config.{AbstractConfig, ConfigResource}
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions.{assertFalse, assertInstanceOf, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag, TestInfo}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Properties
import java.util.concurrent.ExecutionException
import scala.jdk.CollectionConverters._

@Tag("integration")
abstract class BaseAlterConfigsIntegrationTest extends IntegrationTestHarness {
  override def brokerCount = 1

  protected var file: Path = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    file = Files.createTempFile("provider", ".properties")
    Files.write(file, "key=token".getBytes(StandardCharsets.UTF_8))
  }

  @AfterEach
  override def tearDown(): Unit = {
    Files.deleteIfExists(file)
    super.tearDown()
  }

  protected def checkAlterConfigs(adminConfig: Map[String, AnyRef], expectedCause: Class[_ <: Throwable], value: String, placeholder: Boolean): Unit = {
    val props = new Properties()
    props.putAll(adminConfig.asJava)
    val adminClient = createAdminClient(configOverrides = props)
    try {
      val brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "")
      val alterations = Map(brokerResource -> configProviderOps.asJavaCollection).asJava
      val ee = assertThrows(classOf[ExecutionException], () => adminClient.incrementalAlterConfigs(alterations).all.get)
      assertInstanceOf(expectedCause, ee.getCause)
      val message = ee.getCause.getMessage
      assertFalse(message.contains(value))
      if (expectedCause eq classOf[InvalidRequestException]) {
        if (placeholder) assertTrue(message.contains("${file:"))
        else assertTrue(message.contains(classOf[FileConfigProvider].getName + " is not allowed"))
      }
    } finally {
      Utils.closeQuietly(adminClient, "AlterConfigsIntegrationTest")
    }
  }

  protected def configProviderOps: Seq[AlterConfigOp] = List(
    new AlterConfigOp(new ConfigEntry(AbstractConfig.CONFIG_PROVIDERS_CONFIG, "file"), AlterConfigOp.OpType.SET),
    new AlterConfigOp(new ConfigEntry(AbstractConfig.CONFIG_PROVIDERS_CONFIG + ".file.class", classOf[FileConfigProvider].getName), AlterConfigOp.OpType.SET),
    new AlterConfigOp(new ConfigEntry(ServerConfigs.NUM_IO_THREADS_CONFIG, "${file:" + file.toAbsolutePath + ":key}"), AlterConfigOp.OpType.SET)
  )
}

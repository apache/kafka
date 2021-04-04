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
package kafka.admin

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import scala.collection.Seq

class LogDirsCommandTest extends KafkaServerTestHarness {

  def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(1, zkConnect)
      .map(KafkaConfig.fromProps)
  }

  @Test
  def checkLogDirsCommandOutput(): Unit = {
    val byteArrayOutputStream = new ByteArrayOutputStream
    val printStream = new PrintStream(byteArrayOutputStream, false, StandardCharsets.UTF_8.name())
    //input exist brokerList
    LogDirsCommand.describe(Array("--bootstrap-server", brokerList, "--broker-list", "0", "--describe"), printStream)
    val existingBrokersContent = new String(byteArrayOutputStream.toByteArray, StandardCharsets.UTF_8)
    val existingBrokersLineIter = existingBrokersContent.split("\n").iterator

    assertTrue(existingBrokersLineIter.hasNext)
    assertTrue(existingBrokersLineIter.next().contains(s"Querying brokers for log directories information"))

    //input nonexistent brokerList
    byteArrayOutputStream.reset()
    LogDirsCommand.describe(Array("--bootstrap-server", brokerList, "--broker-list", "0,1,2", "--describe"), printStream)
    val nonExistingBrokersContent = new String(byteArrayOutputStream.toByteArray, StandardCharsets.UTF_8)
    val nonExistingBrokersLineIter = nonExistingBrokersContent.split("\n").iterator

    assertTrue(nonExistingBrokersLineIter.hasNext)
    assertTrue(nonExistingBrokersLineIter.next().contains(s"ERROR: The given brokers do not exist from --broker-list: 1,2. Current existent brokers: 0"))

    //input duplicate ids
    byteArrayOutputStream.reset()
    LogDirsCommand.describe(Array("--bootstrap-server", brokerList, "--broker-list", "0,0,1,2,2", "--describe"), printStream)
    val duplicateBrokersContent = new String(byteArrayOutputStream.toByteArray, StandardCharsets.UTF_8)
    val duplicateBrokersLineIter = duplicateBrokersContent.split("\n").iterator

    assertTrue(duplicateBrokersLineIter.hasNext)
    assertTrue(duplicateBrokersLineIter.next().contains(s"ERROR: The given brokers do not exist from --broker-list: 1,2. Current existent brokers: 0"))

    //use all brokerList for current cluster
    byteArrayOutputStream.reset()
    LogDirsCommand.describe(Array("--bootstrap-server", brokerList, "--describe"), printStream)
    val allBrokersContent = new String(byteArrayOutputStream.toByteArray, StandardCharsets.UTF_8)
    val allBrokersLineIter = allBrokersContent.split("\n").iterator

    assertTrue(allBrokersLineIter.hasNext)
    assertTrue(allBrokersLineIter.next().contains(s"Querying brokers for log directories information"))
  }
}

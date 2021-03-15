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
package unit.kafka.admin

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

import kafka.admin.LogDirsCommand
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
    val existBrokersContent = new String(byteArrayOutputStream.toByteArray, StandardCharsets.UTF_8)
    val existBrokersLineIter = existBrokersContent.split("\n").iterator

    assertTrue(existBrokersLineIter.hasNext)
    assertTrue(existBrokersLineIter.next().contains(s"Querying brokers for log directories information"))

    //input nonExist brokerList
    byteArrayOutputStream.reset()
    LogDirsCommand.describe(Array("--bootstrap-server", brokerList, "--broker-list", "0,1,2", "--describe"), printStream)
    val nonExistBrokersContent = new String(byteArrayOutputStream.toByteArray, StandardCharsets.UTF_8)
    val nonExistBrokersLineIter = nonExistBrokersContent.split("\n").iterator

    assertTrue(nonExistBrokersLineIter.hasNext)
    assertTrue(nonExistBrokersLineIter.next().contains(s"ERROR: The given node(s) does not exist from broker-list: 1,2. Current cluster exist node(s): 0"))

    //use all brokerList for current cluster
    byteArrayOutputStream.reset()
    LogDirsCommand.describe(Array("--bootstrap-server", brokerList, "--describe"), printStream)
    val allBrokersContent = new String(byteArrayOutputStream.toByteArray, StandardCharsets.UTF_8)
    val allBrokersLineIter = allBrokersContent.split("\n").iterator

    assertTrue(allBrokersLineIter.hasNext)
    assertTrue(allBrokersLineIter.next().contains(s"Querying brokers for log directories information"))
  }
}

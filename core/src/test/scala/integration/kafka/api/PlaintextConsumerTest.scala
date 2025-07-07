/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.util
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.test.api.Flaky
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.util.concurrent.ExecutionException

@Timeout(60)
class PlaintextConsumerTest extends AbstractConsumerTest {

  @Flaky("KAFKA-18031")
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testCloseLeavesGroupOnInterrupt(groupProtocol: String): Unit = {
    val adminClient = createAdminClient()
    val consumer = createConsumer()
    val listener = new TestConsumerReassignmentListener()
    consumer.subscribe(java.util.List.of(topic), listener)
    awaitRebalance(consumer, listener)

    assertEquals(1, listener.callsToAssigned)
    assertEquals(0, listener.callsToRevoked)

    try {
      Thread.currentThread().interrupt()
      assertThrows(classOf[InterruptException], () => consumer.close())
    } finally {
      // Clear the interrupted flag so we don't create problems for subsequent tests.
      Thread.interrupted()
    }

    assertEquals(1, listener.callsToAssigned)
    assertEquals(1, listener.callsToRevoked)

    val config = new ConsumerConfig(consumerConfig)

    // Set the wait timeout to be only *half* the configured session timeout. This way we can make sure that the
    // consumer explicitly left the group as opposed to being kicked out by the broker.
    val leaveGroupTimeoutMs = config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG) / 2

    TestUtils.waitUntilTrue(
      () => {
        try {
          val groupId = config.getString(ConsumerConfig.GROUP_ID_CONFIG)
          val groupDescription = adminClient.describeConsumerGroups(util.List.of(groupId)).describedGroups.get(groupId).get
          groupDescription.members.isEmpty
        } catch {
          case _: ExecutionException | _: InterruptedException =>
            false
        }
      },
      msg=s"Consumer did not leave the consumer group within $leaveGroupTimeoutMs ms of close",
      waitTimeMs=leaveGroupTimeoutMs
    )
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package integration.kafka.admin

import kafka.server.KafkaRaftServer
import kafka.testkit.KafkaClusterTestKit
import kafka.testkit.TestKitNodes
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, DescribeQuorumOptions}
import org.apache.kafka.metadata.BrokerState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{Tag, Test, Timeout}
import org.slf4j.LoggerFactory


@Timeout(120)
@Tag("integration")
class DescribeQuorumTest {
  val log = LoggerFactory.getLogger(classOf[DescribeQuorumTest])

  @Test
  def testDescribeQuorumRequestToBrokers() = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(4).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format
      cluster.startup
      for (i <- 0 to 3) {
        TestUtils.waitUntilTrue(() => cluster.brokers.get(i).brokerState == BrokerState.RUNNING,
          "Broker Never started up")
      }
      val props = cluster.clientProperties()
      props.put(AdminClientConfig.CLIENT_ID_CONFIG, this.getClass.getName)
      val admin = Admin.create(props)
      try {
        val quorumState = admin.describeQuorum(new DescribeQuorumOptions)
        val quorumInfo = quorumState.quorumInfo().get()

        assertEquals(KafkaRaftServer.MetadataTopic, quorumInfo.topic())
        assertEquals(3, quorumInfo.voters.size())
      } finally {
        admin.close()
      }
    } finally {
      cluster.close
    }
  }
}

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

package kafka.server

import kafka.network.SocketServer
import org.apache.kafka.common.message.{DescribeClusterRequestData, DescribeClusterResponseData}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{DescribeClusterRequest, DescribeClusterResponse}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.security.authorizer.AclEntry
import org.apache.kafka.server.config.ReplicationConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.lang.{Byte => JByte}
import java.util.Properties
import scala.jdk.CollectionConverters._

class DescribeClusterRequestTest extends BaseRequestTest {

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "1")
    properties.setProperty(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "2")
    properties.setProperty(KafkaConfig.RackProp, s"rack/${properties.getProperty(KafkaConfig.BrokerIdProp)}")
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    doSetup(testInfo, createOffsetsTopic = false)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testDescribeClusterRequestIncludingClusterAuthorizedOperations(quorum: String): Unit = {
    testDescribeClusterRequest(true)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testDescribeClusterRequestExcludingClusterAuthorizedOperations(quorum: String): Unit = {
    testDescribeClusterRequest(false)
  }

  def testDescribeClusterRequest(includeClusterAuthorizedOperations: Boolean): Unit = {
    val expectedBrokers = brokers.map { server =>
      new DescribeClusterResponseData.DescribeClusterBroker()
        .setBrokerId(server.config.brokerId)
        .setHost("localhost")
        .setPort(server.socketServer.boundPort(listenerName))
        .setRack(server.config.rack.orNull)
    }.toSet

    var expectedControllerId = 0
    if (!isKRaftTest()) {
      // in KRaft mode DescribeClusterRequest will return a random broker id as the controllerId (KIP-590)
      expectedControllerId = servers.filter(_.kafkaController.isActive).last.config.brokerId
    }
    val expectedClusterId = brokers.last.clusterId

    val expectedClusterAuthorizedOperations = if (includeClusterAuthorizedOperations) {
      Utils.to32BitField(
        AclEntry.supportedOperations(ResourceType.CLUSTER).asScala
          .map(_.code.asInstanceOf[JByte]).asJava)
    } else {
      Int.MinValue
    }

    ensureConsistentKRaftMetadata()

    for (version <- ApiKeys.DESCRIBE_CLUSTER.oldestVersion to ApiKeys.DESCRIBE_CLUSTER.latestVersion) {
      val describeClusterRequest = new DescribeClusterRequest.Builder(new DescribeClusterRequestData()
        .setIncludeClusterAuthorizedOperations(includeClusterAuthorizedOperations))
        .build(version.toShort)
      val describeClusterResponse = sentDescribeClusterRequest(describeClusterRequest)

      if (isKRaftTest()) {
        assertTrue(0 to brokerCount contains describeClusterResponse.data.controllerId)
      } else {
        assertEquals(expectedControllerId, describeClusterResponse.data.controllerId)
      }
      assertEquals(expectedClusterId, describeClusterResponse.data.clusterId)
      assertEquals(expectedClusterAuthorizedOperations, describeClusterResponse.data.clusterAuthorizedOperations)
      assertEquals(expectedBrokers, describeClusterResponse.data.brokers.asScala.toSet)
    }
  }

  private def sentDescribeClusterRequest(request: DescribeClusterRequest, destination: Option[SocketServer] = None): DescribeClusterResponse = {
    connectAndReceive[DescribeClusterResponse](request, destination = destination.getOrElse(anySocketServer))
  }
}

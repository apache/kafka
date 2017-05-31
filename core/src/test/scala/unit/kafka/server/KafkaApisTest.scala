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

package unit.kafka.server


import kafka.api.{ApiVersion, KAFKA_0_10_2_IV0, KAFKA_0_11_0_IV0}
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.network.RequestChannel
import kafka.security.auth.Authorizer
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server._
import kafka.utils.{MockTime, TestUtils, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractRequestResponse, AddPartitionsToTxnRequest, RequestHeader}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Utils
import org.easymock.EasyMock
import org.junit.{Before, Test}

import scala.collection.JavaConverters


class KafkaApisTest {

  private val requestChannel = EasyMock.createNiceMock(classOf[RequestChannel])
  private val replicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])
  private val groupCoordinator = EasyMock.createNiceMock(classOf[GroupCoordinator])
  private val adminManager = EasyMock.createNiceMock(classOf[AdminManager])
  private val txnCoordinator = EasyMock.createNiceMock(classOf[TransactionCoordinator])
  private val controller = EasyMock.createNiceMock(classOf[KafkaController])
  private val zkUtils = EasyMock.createNiceMock(classOf[ZkUtils])
  private val metadataCache = EasyMock.createNiceMock(classOf[MetadataCache])
  private val metrics = new Metrics()
  private val brokerId = 1
  private val authorizer: Option[Authorizer] = None
  private val quotas = EasyMock.createNiceMock(classOf[QuotaManagers])
  private val brokerTopicStats = new BrokerTopicStats
  private val clusterId = "clusterId"
  private val time = new MockTime



  def createKafkaApis(interBrokerProtocolVersion: ApiVersion): KafkaApis = {
    val properties = TestUtils.createBrokerConfig(brokerId, "zk")
    properties.put(KafkaConfig.InterBrokerProtocolVersionProp, interBrokerProtocolVersion.toString)
    properties.put(KafkaConfig.LogMessageFormatVersionProp, interBrokerProtocolVersion.toString)
    new KafkaApis(requestChannel,
      replicaManager,
      adminManager,
      groupCoordinator,
      txnCoordinator,
      controller,
      zkUtils,
      brokerId,
      new KafkaConfig(properties),
      metadataCache,
      metrics,
      authorizer,
      quotas,
      brokerTopicStats,
      clusterId,
      time
    )
  }

  @Test(expected = classOf[UnsupportedVersionException])
  def shouldThrowUnsupportedVersionExceptionOnHandleAddOffsetToTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    createKafkaApis(KAFKA_0_10_2_IV0).handleAddOffsetsToTxnRequest(null)
  }

  @Test(expected = classOf[UnsupportedVersionException])
  def shouldThrowUnsupportedVersionExceptionOnHandleAddPartitionsToTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    createKafkaApis(KAFKA_0_10_2_IV0).handleAddPartitionToTxnRequest(null)
  }

  @Test(expected = classOf[UnsupportedVersionException])
  def shouldThrowUnsupportedVersionExceptionOnHandleTxnOffsetCommitRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    createKafkaApis(KAFKA_0_10_2_IV0).handleAddPartitionToTxnRequest(null)
  }

  @Test(expected = classOf[UnsupportedVersionException])
  def shouldThrowUnsupportedVersionExceptionOnHandleEndTxnRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    createKafkaApis(KAFKA_0_10_2_IV0).handleEndTxnRequest(null)
  }

  @Test(expected = classOf[UnsupportedVersionException])
  def shouldThrowUnsupportedVersionExceptionOnHandleWriteTxnMarkersRequestWhenInterBrokerProtocolNotSupported(): Unit = {
    createKafkaApis(KAFKA_0_10_2_IV0).handleWriteTxnMarkersRequest(null)
  }
  
}

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

package kafka.server.metadata

import kafka.cluster.{Broker, EndPoint}
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.server.{ClientQuotaManager, ClientRequestQuotaManager, ControllerMutationQuotaManager, KafkaConfig, MetadataCache, QuotaFactory, ReplicaManager}
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.metadata.BrokerRecord.BrokerEndpointCollection
import org.apache.kafka.common.metadata.{BrokerRecord, TopicRecord}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.{TopicPartition, UUID}
import org.apache.kafka.server.quota.ClientQuotaCallback
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.Matchers.assertThrows

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class PartitionMetadataProcessorTest {

  @Test
  def testBrokerRecordsAndTopicRecords(): Unit = {
    val kafkaConfig = mock(classOf[KafkaConfig])
    val clusterId = "clusterId"
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val quotaManagers = mock(classOf[QuotaFactory.QuotaManagers])
    val clientQuotaCallbackOption = mock(classOf[Option[ClientQuotaCallback]])
    val clientQuotaCallback = mock(classOf[ClientQuotaCallback])
    val fetchClientQuotaManager = mock(classOf[ClientQuotaManager])
    val requestClientQuotaManager = mock(classOf[ClientRequestQuotaManager])
    val produceClientQuotaManager = mock(classOf[ClientQuotaManager])
    val controllerMutationQuotaManager = mock(classOf[ControllerMutationQuotaManager])
    val listenerName = new ListenerName("foo")
    val metadataCache = new MetadataCache(0)
    val replicaManager = mock(classOf[ReplicaManager])
    val txnCoordinator = mock(classOf[TransactionCoordinator])
    when(quotaManagers.clientQuotaCallback).thenReturn(clientQuotaCallbackOption)
    when(kafkaConfig.interBrokerListenerName).thenReturn(listenerName)
    when(clientQuotaCallbackOption.get).thenReturn(clientQuotaCallback)
    when(clientQuotaCallback.updateClusterMetadata(ArgumentMatchers.any())).thenReturn(true)
    when(quotaManagers.fetch).thenReturn(fetchClientQuotaManager)
    when(quotaManagers.request).thenReturn(requestClientQuotaManager)
    when(quotaManagers.produce).thenReturn(produceClientQuotaManager)
    when(quotaManagers.controllerMutation).thenReturn(controllerMutationQuotaManager)

    val processor = new PartitionMetadataProcessor(
      kafkaConfig, clusterId, metadataCache, groupCoordinator, quotaManagers, replicaManager, txnCoordinator)

    val uuid0 = UUID.randomUUID()
    val name0 = "topic0"
    val topicRecord0 = new TopicRecord().setDeleting(false).setName(name0).setTopicId(uuid0)

    val host0 = "host0"
    val listener0 = "listener0"
    val rack0 = "rack0"
    val port0: Short = 0
    val id0 = 0
    val securityProtocolId0: Short = 0
    val securityProtocol0 = SecurityProtocol.values()(securityProtocolId0)
    val brokerEndpoints0 = new BrokerEndpointCollection()
    val endpoint0 = new BrokerRecord.BrokerEndpoint().setHost(host0).setName(listener0).setPort(port0).setSecurityProtocol(securityProtocolId0)
    brokerEndpoints0.add(endpoint0)
    val brokerRecord0 = new BrokerRecord().setBrokerId(id0).setBrokerEpoch(0).setEndPoints(brokerEndpoints0).setRack(rack0)

    val host1 = "host1"
    val listener1 = "listener1"
    val rack1 = "rack1"
    val port1: Short = 1
    val id1 = 1
    val securityProtocolId1: Short = 1
    val securityProtocol1 = SecurityProtocol.values()(securityProtocolId1)
    val brokerEndpoints1 = new BrokerEndpointCollection()
    val endpoint1 = new BrokerRecord.BrokerEndpoint().setHost(host1).setName(listener1).setPort(port1).setSecurityProtocol(securityProtocolId1)
    brokerEndpoints1.add(endpoint1)
    val brokerRecord1 = new BrokerRecord().setBrokerId(id1).setBrokerEpoch(1).setEndPoints(brokerEndpoints1).setRack(rack1)

    // there should be no change until we process the message
    // confirm no broker-related changes
    assertTrue(metadataCache.readState().aliveBrokers.isEmpty &&
      metadataCache.readState().aliveNodes.isEmpty)
    // confirm no topic-related changes
    assertEquals(0, metadataCache.readState().partitionStates.size)
    assertEquals(0, metadataCache.readState().topicIdMap.size)

    val initialMetadataOffset = -1
    processor.process(MetadataLogEvent(List[ApiMessage](brokerRecord0, topicRecord0, brokerRecord1).asJava, initialMetadataOffset + 10))

    // confirm broker-related changes
    assertEquals(2, metadataCache.readState().aliveBrokers.size)
    assertEquals(2, metadataCache.readState().aliveNodes.size)

    val broker0: Broker = metadataCache.readState().aliveBrokers.get(id0).get
    assertEquals(id0, broker0.id)
    assertEquals(Some(rack0), broker0.rack)
    val listenerName0 = new ListenerName(listener0)
    assertEquals(Seq(new EndPoint(host0, port0, listenerName0, securityProtocol0)), broker0.endPoints)
    val nodes0 = metadataCache.readState().aliveNodes.get(id0).get
    assertEquals(1, nodes0.size)
    val node0 = nodes0.get(listenerName0).get
    assertFalse(node0.hasRack()) // this is the way the code works today, but I'm wondering if this is a bug?
    assertEquals(host0, node0.host())
    assertEquals(id0, node0.id())
    assertEquals(port0, node0.port())

    val broker1: Broker = metadataCache.readState().aliveBrokers.get(id1).get
    assertEquals(id1, broker1.id)
    assertEquals(Some(rack1), broker1.rack)
    val listenerName1 = new ListenerName(listener1)
    assertEquals(Seq(new EndPoint(host1, port1, listenerName1, securityProtocol1)), broker1.endPoints)
    val nodes1 = metadataCache.readState().aliveNodes.get(id1).get
    assertEquals(1, nodes1.size)
    val node1 = nodes1.get(listenerName1).get
    assertFalse(node1.hasRack()) // this is the way the code works today, but I'm wondering if this is a bug?
    assertEquals(host1, node1.host())
    assertEquals(id1, node1.id())
    assertEquals(port1, node1.port())

    // confirm topic-related changes
    assertEquals(1, metadataCache.readState().partitionStates.size)
    val partitionStates0: mutable.LongMap[UpdateMetadataPartitionState] = metadataCache.readState().partitionStates.get(name0).get
    assertEquals(0, partitionStates0.size)
    assertEquals(1, metadataCache.readState().topicIdMap.size)
    assertEquals(name0, metadataCache.readState().topicIdMap.get(uuid0))
    verify(groupCoordinator, times(0)).handleDeletedPartitions(ArgumentMatchers.any())
    verify(clientQuotaCallback, times(0)).updateClusterMetadata(ArgumentMatchers.any())
    verify(fetchClientQuotaManager, times(0)).updateQuotaMetricConfigs()
    verify(requestClientQuotaManager, times(0)).updateQuotaMetricConfigs()
    verify(produceClientQuotaManager, times(0)).updateQuotaMetricConfigs()
    verify(controllerMutationQuotaManager, times(0)).updateQuotaMetricConfigs()

    // manually add a topic-partition so we can test deletion
    partitionStates0.put(0, new UpdateMetadataPartitionState())
    assertEquals(1, metadataCache.readState().partitionStates.size)

    // now add a different topic and delete the first topic as part of a batch
    val uuid1 = UUID.randomUUID()
    val name1 = "topic1"
    val topicRecord1 = new TopicRecord().setDeleting(false).setName(name1).setTopicId(uuid1)
    val topicRecord0Deleting = new TopicRecord().setDeleting(true).setName(name0).setTopicId(uuid0)

    // there should be no change until we process the message
    // confirm no broker-related changes from last update
    assertEquals(2, metadataCache.readState().aliveBrokers.size)
    assertEquals(2, metadataCache.readState().aliveNodes.size)
    // confirm no topic-related changes from last update
    assertEquals(1, metadataCache.readState().topicIdMap.size)
    assertEquals(name0, metadataCache.readState().topicIdMap.get(uuid0))
    assertEquals(1, metadataCache.readState().partitionStates.size)
    assertTrue(metadataCache.readState().partitionStates.get(name0).isDefined)
    assertEquals(1, metadataCache.readState().partitionStates.get(name0).get.size)

    processor.process(MetadataLogEvent(List[ApiMessage](topicRecord0Deleting, topicRecord1).asJava, initialMetadataOffset + 20))

    // confirm still no broker-related changes from last update
    assertEquals(2, metadataCache.readState().aliveBrokers.size)
    assertEquals(2, metadataCache.readState().aliveNodes.size)
    // confirm topic-related changes
    assertEquals(2, metadataCache.readState().topicIdMap.size)
    assertEquals(name0, metadataCache.readState().topicIdMap.get(uuid0))
    assertEquals(name1, metadataCache.readState().topicIdMap.get(uuid1))
    assertEquals(1, metadataCache.readState().partitionStates.size)
    assertTrue(metadataCache.readState().partitionStates.get(name1).isDefined)
    assertEquals(0, metadataCache.readState().partitionStates.get(name1).get.size)
    verify(groupCoordinator, times(1)).handleDeletedPartitions(ArgumentMatchers.eq(Seq(new TopicPartition(name0, 0))))
    verify(clientQuotaCallback, times(1)).updateClusterMetadata(ArgumentMatchers.eq(metadataCache.getClusterMetadata(clusterId, listenerName)))
    verify(fetchClientQuotaManager, times(1)).updateQuotaMetricConfigs()
    verify(requestClientQuotaManager, times(1)).updateQuotaMetricConfigs()
    verify(produceClientQuotaManager, times(1)).updateQuotaMetricConfigs()
    verify(controllerMutationQuotaManager, times(1)).updateQuotaMetricConfigs()
  }

  @Test
  def testOutOfBandRegisterLocalBrokerEvent(): Unit = {
    val processor = createSimpleProcessor()
    val initialBrokerEpoch = -1
    assertEquals(initialBrokerEpoch, processor.brokerEpoch)
    val brokerEpoch = 1
    processor.process(RegisterBrokerEvent(brokerEpoch))
    assertEquals(brokerEpoch, processor.brokerEpoch)
    assertEquals(brokerEpoch, processor.MetadataMgr().getCurrentBrokerEpochs().get(0).get)
    // test idempotency
    processor.process(RegisterBrokerEvent(brokerEpoch))
    assertEquals(brokerEpoch, processor.brokerEpoch)
    assertEquals(brokerEpoch, processor.MetadataMgr().getCurrentBrokerEpochs().get(0).get)
  }

  @Test
  def testDisallowChangeBrokerEpoch(): Unit = {
    val processor = createSimpleProcessor()
    val brokerEpoch = 1
    processor.process(RegisterBrokerEvent(brokerEpoch))
    assertThrows[IllegalArgumentException] { processor.process(RegisterBrokerEvent(brokerEpoch + 1)) }
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testDisallowNegativeBrokerEpoch(): Unit = {
    val processor = createSimpleProcessor()
    processor.process(RegisterBrokerEvent(-1))
  }

  def createSimpleProcessor(): PartitionMetadataProcessor = {
    val kafkaConfig = mock(classOf[KafkaConfig]) // broker ID will be 0
    val clusterId = "clusterId"
    val groupCoordinator = mock(classOf[GroupCoordinator])
    val quotaManagers = mock(classOf[QuotaFactory.QuotaManagers])
    val metadataCache = new MetadataCache(0)
    val replicaManager = mock(classOf[ReplicaManager])
    val txnCoordinator = mock(classOf[TransactionCoordinator])

    new PartitionMetadataProcessor(
      kafkaConfig, clusterId, metadataCache, groupCoordinator, quotaManagers, replicaManager, txnCoordinator)
  }
}

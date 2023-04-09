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

package kafka.coordinator.transaction

import kafka.network.SocketServer
import kafka.server.{IntegrationTestUtils, KafkaConfig}
import kafka.test.annotation.{AutoStart, ClusterTest, ClusterTests, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.{ClusterConfig, ClusterInstance}
import org.apache.kafka.common.message.InitProducerIdRequestData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.{InitProducerIdRequest, InitProducerIdResponse}
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith

import java.util.stream.{Collectors, IntStream}
import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class ProducerIdsIntegrationTest {

  @BeforeEach
  def setup(clusterConfig: ClusterConfig): Unit = {
    clusterConfig.serverProperties().put(KafkaConfig.TransactionsTopicPartitionsProp, "1")
    clusterConfig.serverProperties().put(KafkaConfig.TransactionsTopicReplicationFactorProp, "3")
  }

  @ClusterTests(Array(
    new ClusterTest(clusterType = Type.ZK, brokers = 3, metadataVersion = MetadataVersion.IBP_2_8_IV1),
    new ClusterTest(clusterType = Type.ZK, brokers = 3, metadataVersion = MetadataVersion.IBP_3_0_IV0),
    new ClusterTest(clusterType = Type.KRAFT, brokers = 3, metadataVersion = MetadataVersion.IBP_3_3_IV0)
  ))
  def testUniqueProducerIds(clusterInstance: ClusterInstance): Unit = {
    verifyUniqueIds(clusterInstance)
  }

  @ClusterTest(clusterType = Type.ZK, brokers = 3, autoStart = AutoStart.NO)
  def testUniqueProducerIdsBumpIBP(clusterInstance: ClusterInstance): Unit = {
    clusterInstance.config().serverProperties().put(KafkaConfig.InterBrokerProtocolVersionProp, "2.8")
    clusterInstance.config().brokerServerProperties(0).put(KafkaConfig.InterBrokerProtocolVersionProp, "3.0-IV0")
    clusterInstance.start()
    verifyUniqueIds(clusterInstance)
    clusterInstance.stop()
  }

  private def verifyUniqueIds(clusterInstance: ClusterInstance): Unit = {
    // Request enough PIDs from each broker to ensure each broker generates two PID blocks
    val ids = clusterInstance.brokerSocketServers().stream().flatMap( broker => {
      IntStream.range(0, 1001).parallel().mapToObj( _ => nextProducerId(broker, clusterInstance.clientListener()))
    }).collect(Collectors.toList[Long]).asScala.toSeq

    assertEquals(3003, ids.size, "Expected exactly 3003 IDs")
    assertEquals(ids.size, ids.distinct.size, "Found duplicate producer IDs")
  }

  private def nextProducerId(broker: SocketServer, listener: ListenerName): Long = {
    val data = new InitProducerIdRequestData()
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setTransactionalId(null)
      .setTransactionTimeoutMs(10)
    val request = new InitProducerIdRequest.Builder(data).build()

    val response = IntegrationTestUtils.connectAndReceive[InitProducerIdResponse](request,
      destination = broker,
      listenerName = listener)
    response.data().producerId()
  }
}

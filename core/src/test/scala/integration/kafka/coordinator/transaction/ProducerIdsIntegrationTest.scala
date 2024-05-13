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
import kafka.server.IntegrationTestUtils
import kafka.test.{ClusterConfig, ClusterGenerator, ClusterInstance}
import kafka.test.annotation.{AutoStart, ClusterConfigProperty, ClusterTemplate, ClusterTest, ClusterTestDefaults, ClusterTests, Type}
import kafka.test.junit.ClusterTestExtensions
import org.apache.kafka.common.message.InitProducerIdRequestData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.{InitProducerIdRequest, InitProducerIdResponse}
import org.apache.kafka.server.config.ReplicationConfigs
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{Disabled, Timeout}

import java.util.stream.{Collectors, IntStream}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

object ProducerIdsIntegrationTest {
  def uniqueProducerIdsBumpIBP(clusterGenerator: ClusterGenerator): Unit = {
    val serverProperties = java.util.Collections.singletonMap(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, "2.8")
    val perBrokerProperties: java.util.Map[Integer, java.util.Map[String, String]] =
      java.util.Collections.singletonMap(0,
        java.util.Collections.singletonMap(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, "3.0-IV0"))

    clusterGenerator.accept(ClusterConfig.defaultBuilder()
      .setTypes(Set(Type.ZK).asJava)
      .setBrokers(3)
      .setAutoStart(false)
      .setServerProperties(serverProperties)
      .setPerServerProperties(perBrokerProperties)
      .build())
  }
}

@ClusterTestDefaults(serverProperties = Array(
  new ClusterConfigProperty(key = "transaction.state.log.num.partitions", value = "1")
))
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class ProducerIdsIntegrationTest {

  @ClusterTests(Array(
    new ClusterTest(types = Array(Type.ZK), brokers = 3, metadataVersion = MetadataVersion.IBP_2_8_IV1),
    new ClusterTest(types = Array(Type.ZK), brokers = 3, metadataVersion = MetadataVersion.IBP_3_0_IV0),
    new ClusterTest(types = Array(Type.KRAFT), brokers = 3, metadataVersion = MetadataVersion.IBP_3_3_IV0)
  ))
  def testUniqueProducerIds(clusterInstance: ClusterInstance): Unit = {
    verifyUniqueIds(clusterInstance)
  }

  @ClusterTemplate("uniqueProducerIdsBumpIBP")
  def testUniqueProducerIdsBumpIBP(clusterInstance: ClusterInstance): Unit = {
    clusterInstance.start()
    verifyUniqueIds(clusterInstance)
    clusterInstance.stop()
  }

  @ClusterTest(types = Array(Type.ZK), brokers = 1, autoStart = AutoStart.NO, serverProperties = Array(
    new ClusterConfigProperty(key = "num.io.threads", value = "1")
  ))
  @Timeout(20)
  def testHandleAllocateProducerIdsSingleRequestHandlerThread(clusterInstance: ClusterInstance): Unit = {
    clusterInstance.start()
    verifyUniqueIds(clusterInstance)
    clusterInstance.stop()
  }

  @Disabled // TODO: Enable once producer id block size is configurable (KAFKA-15029)
  @ClusterTest(types = Array(Type.ZK), brokers = 1, autoStart = AutoStart.NO, serverProperties = Array(
    new ClusterConfigProperty(key = "num.io.threads", value = "2")
  ))
  def testMultipleAllocateProducerIdsRequest(clusterInstance: ClusterInstance): Unit = {
    clusterInstance.start()
    verifyUniqueIds(clusterInstance)
    clusterInstance.stop()
  }

  private def verifyUniqueIds(clusterInstance: ClusterInstance): Unit = {
    // Request enough PIDs from each broker to ensure each broker generates two blocks
    val ids = clusterInstance.brokerSocketServers().stream().flatMap( broker => {
      IntStream.range(0, 1001).parallel().mapToObj( _ =>
        nextProducerId(broker, clusterInstance.clientListener())
      )}).collect(Collectors.toList[Long]).asScala.toSeq

    val brokerCount = clusterInstance.brokerIds.size
    val expectedTotalCount = 1001 * brokerCount
    assertEquals(expectedTotalCount, ids.size, s"Expected exactly $expectedTotalCount IDs")
    assertEquals(expectedTotalCount, ids.distinct.size, "Found duplicate producer IDs")
  }

  private def nextProducerId(broker: SocketServer, listener: ListenerName): Long = {
    // Generating producer ids may fail while waiting for the initial block and also
    // when the current block is full and waiting for the prefetched block.
    val deadline = 5.seconds.fromNow
    var shouldRetry = true
    var response: InitProducerIdResponse = null
    while (shouldRetry && deadline.hasTimeLeft()) {
      val data = new InitProducerIdRequestData()
        .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
        .setProducerId(RecordBatch.NO_PRODUCER_ID)
        .setTransactionalId(null)
        .setTransactionTimeoutMs(10)
      val request = new InitProducerIdRequest.Builder(data).build()

      response = IntegrationTestUtils.connectAndReceive[InitProducerIdResponse](request,
        destination = broker,
        listenerName = listener)

      shouldRetry = response.data.errorCode == Errors.COORDINATOR_LOAD_IN_PROGRESS.code
    }
    assertTrue(deadline.hasTimeLeft())
    assertEquals(Errors.NONE.code, response.data.errorCode)
    response.data().producerId()
  }
}

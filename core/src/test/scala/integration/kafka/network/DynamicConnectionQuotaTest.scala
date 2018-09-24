/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.io.IOException
import java.net.{InetAddress, Socket}
import java.util.Properties

import kafka.server.{BaseRequestTest, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

class DynamicConnectionQuotaTest extends BaseRequestTest {

  override def numBrokers = 1

  val topic = "test"

  @Before
  override def setUp(): Unit = {
    super.setUp()
    TestUtils.createTopic(zkClient, topic, numBrokers, numBrokers, servers)
  }

  @Test
  def testDynamicConnectionQuota(): Unit = {
    def connect(socketServer: SocketServer, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT, localAddr: InetAddress = null) = {
      new Socket("localhost", socketServer.boundPort(ListenerName.forSecurityProtocol(protocol)), localAddr, 0)
    }

    val socketServer = servers.head.socketServer
    val localAddress = InetAddress.getByName("127.0.0.1")
    def connectionCount = socketServer.connectionCount(localAddress)
    val initialConnectionCount = connectionCount
    val maxConnectionsPerIP = 5

    val props = new Properties
    props.put(KafkaConfig.MaxConnectionsPerIpProp, maxConnectionsPerIP.toString)
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.MaxConnectionsPerIpProp, maxConnectionsPerIP.toString))

    //wait for adminClient connections to close
    TestUtils.waitUntilTrue(() => initialConnectionCount == connectionCount, "Connection count mismatch")

    //create connections up to maxConnectionsPerIP - 1, leave space for one connection
    var conns = (connectionCount until (maxConnectionsPerIP - 1)).map(_ => connect(socketServer))

    // produce should succeed
    var produceResponse = sendProduceRequest()
    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(Errors.NONE, partitionResponse.error)

    TestUtils.waitUntilTrue(() => connectionCount == (maxConnectionsPerIP - 1), "produce request connection is not closed")
    conns = conns :+ connect(socketServer)
    // now try one more (should fail)
    intercept[IOException](sendProduceRequest())

    conns.foreach(conn => conn.close())
    TestUtils.waitUntilTrue(() => initialConnectionCount == connectionCount, "Connection count mismatch")

    // Increase MaxConnectionsPerIpOverrides for localhost to 7
    val maxConnectionsPerIPOverride = 7
    props.put(KafkaConfig.MaxConnectionsPerIpOverridesProp, s"localhost:$maxConnectionsPerIPOverride")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.MaxConnectionsPerIpOverridesProp, s"localhost:$maxConnectionsPerIPOverride"))

    //wait for adminClient connections to close
    TestUtils.waitUntilTrue(() => initialConnectionCount == connectionCount, "Connection count mismatch")

    //create connections up to maxConnectionsPerIPOverride - 1, leave space for one connection
    conns = (connectionCount until maxConnectionsPerIPOverride - 1).map(_ => connect(socketServer))

    // send should succeed
    produceResponse = sendProduceRequest()
    assertEquals(1, produceResponse.responses.size)
    val (tp1, partitionResponse1) = produceResponse.responses.asScala.head
    assertEquals(Errors.NONE, partitionResponse1.error)

    TestUtils.waitUntilTrue(() => connectionCount == (maxConnectionsPerIPOverride - 1), "produce request connection is not closed")
    conns = conns :+ connect(socketServer)
    // now try one more (should fail)
    intercept[IOException](sendProduceRequest())

    //close one connection
    conns.head.close()
    TestUtils.waitUntilTrue(() => connectionCount == (maxConnectionsPerIPOverride - 1), "connection is not closed")
    // send should succeed
    sendProduceRequest()
  }

  private def reconfigureServers(newProps: Properties, perBrokerConfig: Boolean, aPropToVerify: (String, String)): Unit = {
    val adminClient = createAdminClient()
    TestUtils.alterConfigs(servers, adminClient, newProps, perBrokerConfig).all.get()
    waitForConfigOnServer(aPropToVerify._1, aPropToVerify._2)
    adminClient.close()
  }

  private def createAdminClient(): AdminClient = {
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName(securityProtocol.name))
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "10")
    val adminClient = AdminClient.create(config)
    adminClient
  }

  private def waitForConfigOnServer(propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    TestUtils.retry(maxWaitMs) {
      assertEquals(propValue, servers.head.config.originals.get(propName))
    }
  }

  private def sendProduceRequest(): ProduceResponse = {
    val topicPartition = new TopicPartition(topic, 0)
    val memoryRecords = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes))
    val partitionRecords = Map(topicPartition -> memoryRecords)
    val request = ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build()
    val response = connectAndSend(request, ApiKeys.PRODUCE, servers.head.socketServer)
    ProduceResponse.parse(response, request.version)
  }
}

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

package integration.kafka.api

import java.util
import java.util.Collections

import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.utils.Utils
import org.junit.rules.Timeout
import org.junit.{After, Assert, Before, Rule, Test}

import scala.collection.JavaConverters._


/**
 * Tests changing partition reassignments via the KafkaAdminClient.
 */
class AlterPartitionReassignmentsIntegrationTest extends IntegrationTestHarness with Logging {
  override protected def brokerCount: Int = 5

  @Rule
  def globalTimeout = Timeout.millis(120000)

  override def generateConfigs = {
    val cfgs = TestUtils.createBrokerConfigs(brokerCount, zkConnect)
    cfgs.foreach(_ ++= serverConfig)
    cfgs.map(KafkaConfig.fromProps)
  }

  val ALPHA = "alpha"
  val BETA = "beta"
  val ZETA = "zeta"

  var client: AdminClient = null

  def createAdminClientConfig(): util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.asScala.foreach { case (key, value) => config.put(key.asInstanceOf[String], value) }
    config
  }

  @Before
  override def setUp(): Unit = {
    super.setUp()
    TestUtils.createTopic(zkClient, ALPHA,
      Map(0 -> Seq(0, 1, 2),
          1 -> Seq(1, 2, 3),
          2 -> Seq(2, 3, 0)), servers)
    TestUtils.createTopic(zkClient, BETA,
      Map(0 -> Seq(3),
          1 -> Seq(4)), servers)
    client = AdminClient.create(createAdminClientConfig())
  }

  @After
  override def tearDown(): Unit = {
    if (client != null)
      Utils.closeQuietly(client, "AdminClient")
    super.tearDown()
  }

  private def checkPartitionLocations(topicName: String,
                                      expectedAssignment: collection.Map[Int, Seq[Int]]): Unit = {
    val topicInfos = client.describeTopics(Collections.singleton(topicName))
    val partitionList = topicInfos.values().get(topicName).get().partitions()
    val indexToPartition = partitionList.asScala.map(p => p.partition -> p).toMap
    expectedAssignment.foreach {
      case (partitionIndex, expectedBrokers) =>
        indexToPartition.get(partitionIndex) match {
          case None => throw new RuntimeException(s"No describeTopics entries found for partition ${partitionIndex}")
          case Some(partition) => {
            val brokerIdList = partition.replicas().asScala.map(node => node.id)
            if (!expectedBrokers.equals(brokerIdList)) {
              throw new RuntimeException(s"Expected partition ${partitionIndex} to be on " +
                s"brokers ${expectedBrokers.mkString(", ")}, but it was on brokers ${brokerIdList.mkString(", ")}")
            }
          }
        }
    }
    if (indexToPartition.size > expectedAssignment.size) {
      throw new RuntimeException(s"Found ${indexToPartition.size} partitions in ${topicName}, " +
        s"but only expected ${expectedAssignment.size}")
    }
  }

  @Test
  def testRebalancePartitions(): Unit = {
    val initialReassignments = client.listPartitionReassignments().reassignments().get()
    Assert.assertEquals(0, initialReassignments.size())
    checkPartitionLocations(ALPHA,
        Map(0 -> Seq(0, 1, 2),
            1 -> Seq(1, 2, 3),
            2 -> Seq(2, 3, 0)))
    checkPartitionLocations(BETA,
        Map(0 -> Seq(3),
            1 -> Seq(4)))
  }
}

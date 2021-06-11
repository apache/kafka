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

package kafka.server

import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.metadata.BrokerState
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.api.Assertions._

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

@Timeout(120000)
class RaftClusterTest {

  @Test
  def testCreateClusterAndClose(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
    } finally {
      cluster.close()
    }
  }

  @Test
  def testCreateClusterAndWaitForBrokerInRunningState(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(3).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format()
      cluster.startup()
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).currentState() == BrokerState.RUNNING,
        "Broker never made it to RUNNING state.")
      TestUtils.waitUntilTrue(() => cluster.raftManagers().get(0).kafkaRaftClient.leaderAndEpoch().leaderId.isPresent,
        "RaftManager was not initialized.")
      val admin = Admin.create(cluster.clientProperties())
      try {
        assertEquals(cluster.nodes().clusterId().toString,
          admin.describeCluster().clusterId().get())
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testCreateClusterAndCreateListDeleteTopic(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(3).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).currentState() == BrokerState.RUNNING,
        "Broker never made it to RUNNING state.")
      TestUtils.waitUntilTrue(() => cluster.raftManagers().get(0).kafkaRaftClient.leaderAndEpoch().leaderId.isPresent,
        "RaftManager was not initialized.")

      val admin = Admin.create(cluster.clientProperties())
      try {
        // Create a test topic
        val newTopic = Collections.singletonList(new NewTopic("test-topic", 1, 3.toShort))
        val createTopicResult = admin.createTopics(newTopic)
        createTopicResult.all().get()

        // List created topic
        TestUtils.waitUntilTrue(() => {
          val listTopicsResult = admin.listTopics()
          val result = listTopicsResult.names().get().size() == newTopic.size()
          if (result) {
            newTopic forEach(topic => {
              assertTrue(listTopicsResult.names().get().contains(topic.name()))
            })
          }
          result
        }, "Topics created were not listed.")

        // Delete topic
        val deleteResult = admin.deleteTopics(Collections.singletonList("test-topic"))
        deleteResult.all().get()

        // List again
        TestUtils.waitUntilTrue(() => {
          val listTopicsResult = admin.listTopics()
          val result = listTopicsResult.names().get().size() != newTopic.size()
          if (result) {
            newTopic forEach(topic => {
              assertFalse(listTopicsResult.names().get().contains(topic.name()))
            })
          }
          result
        }, "Topic was not removed from list.")

      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testCreateClusterAndCreateAndManyTopics(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(3).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).currentState() == BrokerState.RUNNING,
        "Broker never made it to RUNNING state.")
      TestUtils.waitUntilTrue(() => cluster.raftManagers().get(0).kafkaRaftClient.leaderAndEpoch().leaderId.isPresent,
        "RaftManager was not initialized.")
      val admin = Admin.create(cluster.clientProperties())
      try {
        // Create many topics
        val newTopic = new util.ArrayList[NewTopic]()
        newTopic.add(new NewTopic("test-topic-1", 1, 3.toShort))
        newTopic.add(new NewTopic("test-topic-2", 1, 3.toShort))
        newTopic.add(new NewTopic("test-topic-3", 1, 3.toShort))
        val createTopicResult = admin.createTopics(newTopic)
        createTopicResult.all().get()

        // List created topic
        TestUtils.waitUntilTrue(() => {
          val listTopicsResult = admin.listTopics()
          val result = listTopicsResult.names().get().size() == newTopic.size()
          if (result) {
            newTopic forEach(topic => {
              assertTrue(listTopicsResult.names().get().contains(topic.name()))
            })
          }
          result
        }, "Topics created were not listed.")
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testCreateClusterAndCreateAndManyTopicsWithManyPartitions(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(3).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).currentState() == BrokerState.RUNNING,
        "Broker never made it to RUNNING state.")
      TestUtils.waitUntilTrue(() => cluster.raftManagers().get(0).kafkaRaftClient.leaderAndEpoch().leaderId.isPresent,
        "RaftManager was not initialized.")
      val admin = Admin.create(cluster.clientProperties())
      try {
        // Create many topics
        val newTopic = new util.ArrayList[NewTopic]()
        newTopic.add(new NewTopic("test-topic-1", 3, 3.toShort))
        newTopic.add(new NewTopic("test-topic-2", 3, 3.toShort))
        newTopic.add(new NewTopic("test-topic-3", 3, 3.toShort))
        val createTopicResult = admin.createTopics(newTopic)
        createTopicResult.all().get()

        // List created topic
        TestUtils.waitUntilTrue(() => {
          val listTopicsResult = admin.listTopics()
          val result = listTopicsResult.names().get().size() == newTopic.size()
          if (result) {
            newTopic forEach(topic => {
              assertTrue(listTopicsResult.names().get().contains(topic.name()))
            })
          }
          result
        }, "Topics created were not listed.")
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testClientQuotas(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).currentState() == BrokerState.RUNNING,
        "Broker never made it to RUNNING state.")
      val admin = Admin.create(cluster.clientProperties())
      try {
        val entity = new ClientQuotaEntity(Map("user" -> "testkit").asJava)
        var filter = ClientQuotaFilter.containsOnly(
          List(ClientQuotaFilterComponent.ofEntity("user", "testkit")).asJava)

        def alterThenDescribe(entity: ClientQuotaEntity,
                              quotas: Seq[ClientQuotaAlteration.Op],
                              filter: ClientQuotaFilter,
                              expectCount: Int): java.util.Map[ClientQuotaEntity, java.util.Map[String, java.lang.Double]] = {
          val alterResult = admin.alterClientQuotas(Seq(new ClientQuotaAlteration(entity, quotas.asJava)).asJava)
          try {
            alterResult.all().get()
          } catch {
            case t: Throwable => fail("AlterClientQuotas request failed", t)
          }

          def describeOrFail(filter: ClientQuotaFilter): java.util.Map[ClientQuotaEntity, java.util.Map[String, java.lang.Double]] = {
            try {
              admin.describeClientQuotas(filter).entities().get()
            } catch {
              case t: Throwable => fail("DescribeClientQuotas request failed", t)
            }
          }

          val (describeResult, ok) = TestUtils.computeUntilTrue(describeOrFail(filter)) {
            results => results.getOrDefault(entity, java.util.Collections.emptyMap[String, java.lang.Double]()).size() == expectCount
          }
          assertTrue(ok, "Broker never saw new client quotas")
          describeResult
        }

        var describeResult = alterThenDescribe(entity,
          Seq(new ClientQuotaAlteration.Op("request_percentage", 0.99)), filter, 1)
        assertEquals(0.99, describeResult.get(entity).get("request_percentage"), 1e-6)

        describeResult = alterThenDescribe(entity, Seq(
          new ClientQuotaAlteration.Op("request_percentage", 0.97),
          new ClientQuotaAlteration.Op("producer_byte_rate", 10000),
          new ClientQuotaAlteration.Op("consumer_byte_rate", 10001)
        ), filter, 3)
        assertEquals(0.97, describeResult.get(entity).get("request_percentage"), 1e-6)
        assertEquals(10000.0, describeResult.get(entity).get("producer_byte_rate"), 1e-6)
        assertEquals(10001.0, describeResult.get(entity).get("consumer_byte_rate"), 1e-6)

        describeResult = alterThenDescribe(entity, Seq(
          new ClientQuotaAlteration.Op("request_percentage", 0.95),
          new ClientQuotaAlteration.Op("producer_byte_rate", null),
          new ClientQuotaAlteration.Op("consumer_byte_rate", null)
        ), filter, 1)
        assertEquals(0.95, describeResult.get(entity).get("request_percentage"), 1e-6)

        describeResult = alterThenDescribe(entity, Seq(
          new ClientQuotaAlteration.Op("request_percentage", null)), filter, 0)

        describeResult = alterThenDescribe(entity,
          Seq(new ClientQuotaAlteration.Op("producer_byte_rate", 9999)), filter, 1)
        assertEquals(9999.0, describeResult.get(entity).get("producer_byte_rate"), 1e-6)

        // Add another quota for a different entity with same user part
        val entity2 = new ClientQuotaEntity(Map("user" -> "testkit", "client-id" -> "some-client").asJava)
        filter = ClientQuotaFilter.containsOnly(
          List(
            ClientQuotaFilterComponent.ofEntity("user", "testkit"),
            ClientQuotaFilterComponent.ofEntity("client-id", "some-client"),
          ).asJava)
        describeResult = alterThenDescribe(entity2,
          Seq(new ClientQuotaAlteration.Op("producer_byte_rate", 9998)), filter, 1)
        assertEquals(9998.0, describeResult.get(entity2).get("producer_byte_rate"), 1e-6)

        // non-strict match
        filter = ClientQuotaFilter.contains(
          List(ClientQuotaFilterComponent.ofEntity("user", "testkit")).asJava)

        TestUtils.tryUntilNoAssertionError(){
          val results = admin.describeClientQuotas(filter).entities().get()
          assertEquals(2, results.size(), "Broker did not see two client quotas")
          assertEquals(9999.0, results.get(entity).get("producer_byte_rate"), 1e-6)
          assertEquals(9998.0, results.get(entity2).get("producer_byte_rate"), 1e-6)
        }
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }
}

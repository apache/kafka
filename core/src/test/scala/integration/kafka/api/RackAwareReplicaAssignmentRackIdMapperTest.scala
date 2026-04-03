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

package integration.kafka.api

import kafka.admin.{IgnorePrefixRackIdMapper, RackAwareReplicaAssignmentRackIdMapper}
import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.CoreUtils
import kafka.utils.TestUtils.{adminClientSecurityConfigs, waitUntilTrue}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.{AfterEach, Assertions, Test}

import java.util.Properties
import scala.collection.JavaConverters._

class RackAwareReplicaAssignmentRackIdMapperTest extends IntegrationTestHarness {
  val rackIdMapperClassName = classOf[IgnorePrefixRackIdMapper].getCanonicalName
  val rackIdMapper = CoreUtils.createObject[RackAwareReplicaAssignmentRackIdMapper](rackIdMapperClassName)
  val brokerRacks = List(
    "A::rack1",
    "B::rack1",
    "B::rack2",
    "C::rack3",
  )

  var client: Admin = _

  override val brokerCount = brokerRacks.size

  @AfterEach
  override def tearDown(): Unit = {
    if (client != null) {
      Utils.closeQuietly(client, "AdminClient")
      client = null
    }
    super.tearDown()
  }

  override def modifyConfigs(configs: Seq[Properties]): Unit = {
    super.modifyConfigs(configs)
    // Specify customized rack.id
    configs.zip(brokerRacks).foreach { case (cfgForEachBroker, rackId) =>
      cfgForEachBroker.put(KafkaConfig.RackProp, rackId)
      cfgForEachBroker.put(KafkaConfig.LiRackIdMapperClassNameForRackAwareReplicaAssignmentProp, rackIdMapperClassName)
    }
  }

  def createConfig: java.util.Map[String, Object] = {
    val config = new java.util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
    val securityProps: java.util.Map[Object, Object] =
      adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.forEach { (key, value) => config.put(key.asInstanceOf[String], value) }
    config
  }

  def waitForTopics(client: Admin, expectedPresent: Seq[String], expectedMissing: Seq[String]): Unit = {
    waitUntilTrue(() => {
      val topics = client.listTopics.names.get()
      expectedPresent.forall(topicName => topics.contains(topicName)) &&
        expectedMissing.forall(topicName => !topics.contains(topicName))
    }, "timed out waiting for topics")
  }


  /**
   * The rack is prefixed with some extra info, and the mapper should "ignore" the prefix.
   * This is to simulate the case that we want to a) encode datacenter cages and b) group racks across cages
   *
   * If the mapper is not honored, with 8 partitions & RF=3, the underlying logic would place
   *    8 (partitions) * 3 (RF) / 4 (brokers) = 6 replicas
   * on each broker, because all of them are viewed as of different rack.
   * The "mapped rack" rack1 has 2 brokers (0 & 1), which would hold 12 replicas under such situation,
   * and by the pigeon hole theorem, there will be at least 1 partition having multiple replicas on the "mapped rack" rack1.
   *
   * On the other hand, if the mapper is honored, the broker will treat broker 0 & 1 as they're on the same rack
   * and avoid placing the same partition on them.
   */
  @Test
  def testCreateTopicWithExtraRackIdMapper(): Unit = {
    val topicName = "mytopic"
    val newTopics = Seq(new NewTopic(topicName, 8, 3.toShort))

    client = Admin.create(createConfig)

    client.createTopics(newTopics.asJava).all.get()
    waitForTopics(client, Seq(topicName), Seq())

    val assignment =
      client.describeTopics(Seq(topicName).asJava).values().get(topicName).get()
        .partitions().asScala
        // Flatten each replica node in every partition as (Node -> Partition)
        .flatMap(tp => tp.replicas().asScala.map(_ -> tp.partition()))
        // Group by mapped rack.id, now we have mappedRackedId -> [<Node, partitions> on the rack]
        .groupBy(x => rackIdMapper(x._1.rack))


    for ((mappedRack, placementOnRack) <- assignment) { // For each rack
      Assertions.assertEquals(
        placementOnRack.size,                           // should remain the same size for the placement
        placementOnRack.map(_._2) .toSet.size,          // after extracting the partition of that placement and deduplicate by partition
        s"Mapped rack ${mappedRack} holds multiple replica of the same partitions.  Mapper not honored or assignment logic flaw.\n"
          + s"On rack: ${placementOnRack}\n"
          + s"Full assignment: ${assignment}"
      )
    }
  }

}

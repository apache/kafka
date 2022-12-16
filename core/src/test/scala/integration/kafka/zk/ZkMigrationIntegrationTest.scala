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
package kafka.zk

import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.ZkClusterInvocationContext.ZkClusterInstance
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.migration.ZkMigrationLeadershipState
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.extension.ExtendWith

import java.util
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class ZkMigrationIntegrationTest {

  class MetadataDeltaVerifier {
    val metadataDelta = new MetadataDelta(MetadataImage.EMPTY)
    var offset = 0
    def accept(batch: java.util.List[ApiMessageAndVersion]): Unit = {
      batch.forEach(message => {
        metadataDelta.replay(message.message())
        offset += 1
      })
    }

    def verify(verifier: MetadataImage => Unit): Unit = {
      val image = metadataDelta.apply(new MetadataProvenance(offset, 0, 0))
      verifier.apply(image)
    }
  }

  @ClusterTest(brokers = 3, clusterType = Type.ZK, metadataVersion = MetadataVersion.IBP_3_4_IV0)
  def testMigrate(clusterInstance: ClusterInstance): Unit = {
    val admin = clusterInstance.createAdminClient()
    val newTopics = new util.ArrayList[NewTopic]()
    newTopics.add(new NewTopic("test-topic-1", 2, 3.toShort)
      .configs(Map(TopicConfig.SEGMENT_BYTES_CONFIG -> "102400", TopicConfig.SEGMENT_MS_CONFIG -> "300000").asJava))
    newTopics.add(new NewTopic("test-topic-2", 1, 3.toShort))
    newTopics.add(new NewTopic("test-topic-3", 10, 3.toShort))
    val createTopicResult = admin.createTopics(newTopics)
    createTopicResult.all().get(60, TimeUnit.SECONDS)

    val quotas = new util.ArrayList[ClientQuotaAlteration]()
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("user" -> "user1").asJava),
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 1000.0)).asJava))
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("user" -> "user1", "client-id" -> "clientA").asJava),
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 800.0), new ClientQuotaAlteration.Op("producer_byte_rate", 100.0)).asJava))
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("ip" -> "8.8.8.8").asJava),
      List(new ClientQuotaAlteration.Op("connection_creation_rate", 10.0)).asJava))
    admin.alterClientQuotas(quotas)

    val zkClient = clusterInstance.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient
    val migrationClient = new ZkMigrationClient(zkClient)
    var migrationState = migrationClient.getOrCreateMigrationRecoveryState(ZkMigrationLeadershipState.EMPTY)
    migrationState = migrationState.withNewKRaftController(3000, 42)
    migrationState = migrationClient.claimControllerLeadership(migrationState)

    val brokers = new java.util.HashSet[Integer]()
    val verifier = new MetadataDeltaVerifier()
    migrationClient.readAllMetadata(batch => verifier.accept(batch), brokerId => brokers.add(brokerId))
    assertEquals(Seq(0, 1, 2), brokers.asScala.toSeq)

    verifier.verify { image =>
      assertNotNull(image.topics().getTopic("test-topic-1"))
      assertEquals(2, image.topics().getTopic("test-topic-1").partitions().size())

      assertNotNull(image.topics().getTopic("test-topic-2"))
      assertEquals(1, image.topics().getTopic("test-topic-2").partitions().size())

      assertNotNull(image.topics().getTopic("test-topic-3"))
      assertEquals(10, image.topics().getTopic("test-topic-3").partitions().size())

      val clientQuotas = image.clientQuotas().entities()
      assertEquals(3, clientQuotas.size())
    }

    migrationState = migrationClient.releaseControllerLeadership(migrationState)
  }
}

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

import java.net.InetAddress
import java.util
import java.util.concurrent.{ExecutionException, TimeUnit}
import org.apache.kafka.common.test.api.ClusterInstance
import org.apache.kafka.common.test.api.ClusterTest
import org.apache.kafka.common.test.api.ClusterTestExtensions
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{ScramCredentialInfo, ScramMechanism, UserScramCredentialUpsertion}
import org.apache.kafka.common.errors.{InvalidRequestException, UnsupportedVersionException}
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.common.requests.{AlterClientQuotasRequest, AlterClientQuotasResponse, DescribeClientQuotasRequest, DescribeClientQuotasResponse}
import org.apache.kafka.server.config.{QuotaConfig, ZooKeeperInternals}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.extension.ExtendWith

import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class ClientQuotasRequestTest(cluster: ClusterInstance) {
  @ClusterTest
  def testAlterClientQuotasRequest(): Unit = {

    val entity = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> "user", ClientQuotaEntity.CLIENT_ID -> "client-id").asJava)

    // Expect an empty configuration.
    verifyDescribeEntityQuotas(entity, Map.empty)

    // Add two configuration entries.
    alterEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> Some(10000.0),
      QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> Some(20000.0)
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 10000.0,
      QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0
    ))

    // Update an existing entry.
    alterEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> Some(15000.0)
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 15000.0,
      QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0
    ))

    // Remove an existing configuration entry.
    alterEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> None
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0
    ))

    // Remove a non-existent configuration entry.  This should make no changes.
    alterEntityQuotas(entity, Map(
      QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> None
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0
    ))

    // Add back a deleted configuration entry.
    alterEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> Some(5000.0)
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 5000.0,
      QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0
    ))

    // Perform a mixed update.
    alterEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> Some(20000.0),
      QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> None,
      QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> Some(12.3)
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0,
      QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> 12.3
    ))
  }

  @ClusterTest
  def testAlterClientQuotasRequestValidateOnly(): Unit = {
    val entity = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> "user").asJava)

    // Set up a configuration.
    alterEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> Some(20000.0),
      QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> Some(23.45)
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0,
      QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> 23.45
    ))

    // Validate-only addition.
    alterEntityQuotas(entity, Map(
      QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> Some(50000.0)
    ), validateOnly = true)

    verifyDescribeEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0,
      QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> 23.45
    ))

    // Validate-only modification.
    alterEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> Some(10000.0)
    ), validateOnly = true)

    verifyDescribeEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0,
      QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> 23.45
    ))

    // Validate-only removal.
    alterEntityQuotas(entity, Map(
      QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> None
    ), validateOnly = true)

    verifyDescribeEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0,
      QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> 23.45
    ))

    // Validate-only mixed update.
    alterEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> Some(10000.0),
      QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> Some(50000.0),
      QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> None
    ), validateOnly = true)

    verifyDescribeEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0,
      QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> 23.45
    ))
  }

  @Disabled("TODO: KAFKA-17630 -  Convert ClientQuotasRequestTest#testClientQuotasForScramUsers to kraft")
  @ClusterTest
  def testClientQuotasForScramUsers(): Unit = {
    val userName = "user"

    val admin = cluster.admin()
    try {
      val results = admin.alterUserScramCredentials(util.Arrays.asList(
        new UserScramCredentialUpsertion(userName, new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), "password")))
      results.all.get

      val entity = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> userName).asJava)

      verifyDescribeEntityQuotas(entity, Map.empty)

      alterEntityQuotas(entity, Map(
        QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> Some(10000.0),
        QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> Some(20000.0)
      ), validateOnly = false)

      verifyDescribeEntityQuotas(entity, Map(
        QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 10000.0,
        QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0
      ))
    } finally {
      admin.close()
    }
  }

  @ClusterTest
  def testAlterIpQuotasRequest(): Unit = {
    val knownHost = "1.2.3.4"
    val unknownHost = "2.3.4.5"
    val entity = toIpEntity(Some(knownHost))
    val defaultEntity = toIpEntity(Some(null))
    val entityFilter = ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.IP, knownHost)
    val defaultEntityFilter = ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.IP)
    val allIpEntityFilter = ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.IP)

    def verifyIpQuotas(entityFilter: ClientQuotaFilterComponent, expectedMatches: Map[ClientQuotaEntity, Double]): Unit = {
      TestUtils.tryUntilNoAssertionError() {
        val result = describeClientQuotas(ClientQuotaFilter.containsOnly(List(entityFilter).asJava))
        assertEquals(expectedMatches.keySet, result.asScala.keySet)
        result.asScala.foreach { case (entity, props) =>
          assertEquals(Set(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG), props.asScala.keySet)
          assertEquals(expectedMatches(entity), props.get(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG))
          val entityName = entity.entries.get(ClientQuotaEntity.IP)
          // ClientQuotaEntity with null name maps to default entity
          val entityIp = if (entityName == null)
            InetAddress.getByName(unknownHost)
          else
            InetAddress.getByName(entityName)
          var currentServerQuota = 0
          currentServerQuota = cluster.brokerSocketServers().asScala.head.connectionQuotas.connectionRateForIp(entityIp)
          assertTrue(Math.abs(expectedMatches(entity) - currentServerQuota) < 0.01,
            s"Connection quota of $entity is not ${expectedMatches(entity)} but $currentServerQuota")
        }
      }
    }

    // Expect an empty configuration.
    verifyIpQuotas(allIpEntityFilter, Map.empty)

    // Add a configuration entry.
    alterEntityQuotas(entity, Map(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG -> Some(100.0)), validateOnly = false)
    verifyIpQuotas(entityFilter, Map(entity -> 100.0))

    // update existing entry
    alterEntityQuotas(entity, Map(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG -> Some(150.0)), validateOnly = false)
    verifyIpQuotas(entityFilter, Map(entity -> 150.0))

    // update default value
    alterEntityQuotas(defaultEntity, Map(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG -> Some(200.0)), validateOnly = false)
    verifyIpQuotas(defaultEntityFilter, Map(defaultEntity -> 200.0))

    // describe all IP quotas
    verifyIpQuotas(allIpEntityFilter, Map(entity -> 150.0, defaultEntity -> 200.0))

    // remove entry
    alterEntityQuotas(entity, Map(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG -> None), validateOnly = false)
    verifyIpQuotas(entityFilter, Map.empty)

    // remove default value
    alterEntityQuotas(defaultEntity, Map(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG -> None), validateOnly = false)
    verifyIpQuotas(allIpEntityFilter, Map.empty)
  }

  @ClusterTest
  def testAlterClientQuotasInvalidRequests(): Unit = {
    var entity = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> "").asJava)
    assertThrows(classOf[InvalidRequestException], () => alterEntityQuotas(entity, Map(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> Some(12.34)), validateOnly = true))

    entity = new ClientQuotaEntity(Map(ClientQuotaEntity.CLIENT_ID -> "").asJava)
    assertThrows(classOf[InvalidRequestException], () => alterEntityQuotas(entity, Map(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> Some(12.34)), validateOnly = true))

    entity = new ClientQuotaEntity(Map("" -> "name").asJava)
    assertThrows(classOf[InvalidRequestException], () => alterEntityQuotas(entity, Map(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> Some(12.34)), validateOnly = true))

    entity = new ClientQuotaEntity(Map.empty.asJava)
    assertThrows(classOf[InvalidRequestException], () => alterEntityQuotas(entity, Map(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> Some(10000.5)), validateOnly = true))

    entity = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> "user").asJava)
    assertThrows(classOf[InvalidRequestException], () => alterEntityQuotas(entity, Map("bad" -> Some(1.0)), validateOnly = true))

    entity = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> "user").asJava)
    assertThrows(classOf[InvalidRequestException], () => alterEntityQuotas(entity, Map(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> Some(10000.5)), validateOnly = true))
  }

  private def expectInvalidRequestWithMessage(runnable: => Unit, expectedMessage: String): Unit = {
    val exception = assertThrows(classOf[InvalidRequestException], () => runnable)
    assertTrue(exception.getMessage.contains(expectedMessage), s"Expected message $exception to contain $expectedMessage")
  }

  @ClusterTest
  def testAlterClientQuotasInvalidEntityCombination(): Unit = {
    val userAndIpEntity = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> "user", ClientQuotaEntity.IP -> "1.2.3.4").asJava)
    val clientAndIpEntity = new ClientQuotaEntity(Map(ClientQuotaEntity.CLIENT_ID -> "client", ClientQuotaEntity.IP -> "1.2.3.4").asJava)
    val expectedExceptionMessage = "Invalid quota entity combination"
    expectInvalidRequestWithMessage(alterEntityQuotas(userAndIpEntity, Map(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> Some(12.34)),
      validateOnly = true), expectedExceptionMessage)
    expectInvalidRequestWithMessage(alterEntityQuotas(clientAndIpEntity, Map(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG -> Some(12.34)),
      validateOnly = true), expectedExceptionMessage)
  }

  @ClusterTest
  def testAlterClientQuotasBadIp(): Unit = {
    val invalidHostPatternEntity = new ClientQuotaEntity(Map(ClientQuotaEntity.IP -> "not a valid host because it has spaces").asJava)
    val unresolvableHostEntity = new ClientQuotaEntity(Map(ClientQuotaEntity.IP ->  "RFC2606.invalid").asJava)
    val expectedExceptionMessage = "not a valid IP"
    expectInvalidRequestWithMessage(alterEntityQuotas(invalidHostPatternEntity, Map(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG -> Some(50.0)),
      validateOnly = true), expectedExceptionMessage)
    expectInvalidRequestWithMessage(alterEntityQuotas(unresolvableHostEntity, Map(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG -> Some(50.0)),
      validateOnly = true), expectedExceptionMessage)
  }

  @ClusterTest
  def testDescribeClientQuotasInvalidFilterCombination(): Unit = {
    val ipFilterComponent = ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.IP)
    val userFilterComponent = ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER)
    val clientIdFilterComponent = ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.CLIENT_ID)
    val expectedExceptionMessage = "Invalid entity filter component combination"
    expectInvalidRequestWithMessage(describeClientQuotas(ClientQuotaFilter.contains(List(ipFilterComponent, userFilterComponent).asJava)),
      expectedExceptionMessage)
    expectInvalidRequestWithMessage(describeClientQuotas(ClientQuotaFilter.contains(List(ipFilterComponent, clientIdFilterComponent).asJava)),
      expectedExceptionMessage)
  }

  // Entities to be matched against.
  private val matchUserClientEntities = List(
    (Some("user-1"), Some("client-id-1"), 50.50),
    (Some("user-2"), Some("client-id-1"), 51.51),
    (Some("user-3"), Some("client-id-2"), 52.52),
    (Some(null), Some("client-id-1"), 53.53),
    (Some("user-1"), Some(null), 54.54),
    (Some("user-3"), Some(null), 55.55),
    (Some("user-1"), None, 56.56),
    (Some("user-2"), None, 57.57),
    (Some("user-3"), None, 58.58),
    (Some(null), None, 59.59),
    (None, Some("client-id-2"), 60.60)
  ).map { case (u, c, v) => (toClientEntity(u, c), v) }

  private val matchIpEntities = List(
    (Some("1.2.3.4"), 10.0),
    (Some("2.3.4.5"), 20.0)
  ).map { case (ip, quota) => (toIpEntity(ip), quota)}

  private def setupDescribeClientQuotasMatchTest(): Unit = {
    val userClientQuotas = matchUserClientEntities.map { case (e, v) =>
      e -> Map((QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, Some(v)))
    }.toMap
    val ipQuotas = matchIpEntities.map { case (e, v) =>
      e -> Map((QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG, Some(v)))
    }.toMap
    val result = alterClientQuotas(userClientQuotas ++ ipQuotas, validateOnly = false)
    (matchUserClientEntities ++ matchIpEntities).foreach(e => result(e._1).get(10, TimeUnit.SECONDS))
  }

  @ClusterTest
  def testDescribeClientQuotasMatchExact(): Unit = {
    setupDescribeClientQuotasMatchTest()

    def matchEntity(entity: ClientQuotaEntity) = {
      val components = entity.entries.asScala.map { case (entityType, entityName) =>
        entityName match {
          case null => ClientQuotaFilterComponent.ofDefaultEntity(entityType)
          case name => ClientQuotaFilterComponent.ofEntity(entityType, name)
        }
      }
      describeClientQuotas(ClientQuotaFilter.containsOnly(components.toList.asJava))
    }

    // Test exact matches.
    matchUserClientEntities.foreach { case (e, v) =>
      TestUtils.tryUntilNoAssertionError() {
        val result = matchEntity(e)
        assertEquals(1, result.size)
        assertTrue(result.get(e) != null)
        val value = result.get(e).get(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG)
        assertNotNull(value)
        assertEquals(value, v, 1e-6)
      }
    }

    // Entities not contained in `matchEntityList`.
    val notMatchEntities = List(
      (Some("user-1"), Some("client-id-2")),
      (Some("user-3"), Some("client-id-1")),
      (Some("user-2"), Some(null)),
      (Some("user-4"), None),
      (Some(null), Some("client-id-2")),
      (None, Some("client-id-1")),
      (None, Some("client-id-3")),
    ).map { case (u, c) =>
        new ClientQuotaEntity((u.map((ClientQuotaEntity.USER, _)) ++
          c.map((ClientQuotaEntity.CLIENT_ID, _))).toMap.asJava)
    }

    // Verify exact matches of the non-matches returns empty.
    notMatchEntities.foreach { e =>
      val result = matchEntity(e)
      assertEquals(0, result.size)
    }
  }

  @ClusterTest
  def testDescribeClientQuotasMatchPartial(): Unit = {
    setupDescribeClientQuotasMatchTest()

    def testMatchEntities(filter: ClientQuotaFilter, expectedMatchSize: Int, partition: ClientQuotaEntity => Boolean): Unit = {
      TestUtils.tryUntilNoAssertionError() {
        val result = describeClientQuotas(filter)
        val (expectedMatches, _) = (matchUserClientEntities ++ matchIpEntities).partition(e => partition(e._1))
        assertEquals(expectedMatchSize, expectedMatches.size)  // for test verification
        assertEquals(expectedMatchSize, result.size, s"Failed to match $expectedMatchSize entities for $filter")
        val expectedMatchesMap = expectedMatches.toMap
        matchUserClientEntities.foreach { case (entity, expectedValue) =>
          if (expectedMatchesMap.contains(entity)) {
            val config = result.get(entity)
            assertNotNull(config)
            val value = config.get(QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG)
            assertNotNull(value)
            assertEquals(expectedValue, value, 1e-6)
          } else {
            assertNull(result.get(entity))
          }
        }
        matchIpEntities.foreach { case (entity, expectedValue) =>
          if (expectedMatchesMap.contains(entity)) {
            val config = result.get(entity)
            assertNotNull(config)
            val value = config.get(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG)
            assertNotNull(value)
            assertEquals(expectedValue, value, 1e-6)
          } else {
            assertNull(result.get(entity))
          }
        }
      }
    }

    // Match open-ended existing user.
    testMatchEntities(
      ClientQuotaFilter.contains(List(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user-1")).asJava), 3,
      entity => entity.entries.get(ClientQuotaEntity.USER) == "user-1"
    )

    // Match open-ended non-existent user.
    testMatchEntities(
      ClientQuotaFilter.contains(List(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "unknown")).asJava), 0,
      entity => false
    )

    // Match open-ended existing client ID.
    testMatchEntities(
      ClientQuotaFilter.contains(List(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.CLIENT_ID, "client-id-2")).asJava), 2,
      entity => entity.entries.get(ClientQuotaEntity.CLIENT_ID) == "client-id-2"
    )

    // Match open-ended default user.
    testMatchEntities(
      ClientQuotaFilter.contains(List(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER)).asJava), 2,
      entity => entity.entries.containsKey(ClientQuotaEntity.USER) && entity.entries.get(ClientQuotaEntity.USER) == null
    )

    // Match close-ended existing user.
    testMatchEntities(
      ClientQuotaFilter.containsOnly(List(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user-2")).asJava), 1,
      entity => entity.entries.get(ClientQuotaEntity.USER) == "user-2" && !entity.entries.containsKey(ClientQuotaEntity.CLIENT_ID)
    )

    // Match close-ended existing client ID that has no matching entity.
    testMatchEntities(
      ClientQuotaFilter.containsOnly(List(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.CLIENT_ID, "client-id-1")).asJava), 0,
      entity => false
    )

    // Match against all entities with the user type in a close-ended match.
    testMatchEntities(
      ClientQuotaFilter.containsOnly(List(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER)).asJava), 4,
      entity => entity.entries.containsKey(ClientQuotaEntity.USER) && !entity.entries.containsKey(ClientQuotaEntity.CLIENT_ID)
    )

    // Match against all entities with the user type in an open-ended match.
    testMatchEntities(
      ClientQuotaFilter.contains(List(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER)).asJava), 10,
      entity => entity.entries.containsKey(ClientQuotaEntity.USER)
    )

    // Match against all entities with the client ID type in a close-ended match.
    testMatchEntities(
      ClientQuotaFilter.containsOnly(List(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.CLIENT_ID)).asJava), 1,
      entity => entity.entries.containsKey(ClientQuotaEntity.CLIENT_ID) && !entity.entries.containsKey(ClientQuotaEntity.USER)
    )

    // Match against all entities with the client ID type in an open-ended match.
    testMatchEntities(
      ClientQuotaFilter.contains(List(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.CLIENT_ID)).asJava),  7,
      entity => entity.entries.containsKey(ClientQuotaEntity.CLIENT_ID)
    )

    // Match against all entities with IP type in an open-ended match.
    testMatchEntities(
      ClientQuotaFilter.contains(List(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.IP)).asJava), 2,
      entity => entity.entries.containsKey(ClientQuotaEntity.IP)
    )

    // Match open-ended empty filter list. This should match all entities.
    testMatchEntities(ClientQuotaFilter.contains(List.empty.asJava), 13, entity => true)

    // Match close-ended empty filter list. This should match no entities.
    testMatchEntities(ClientQuotaFilter.containsOnly(List.empty.asJava), 0, _ => false)
  }

  @ClusterTest
  def testClientQuotasUnsupportedEntityTypes(): Unit = {
    val entity = new ClientQuotaEntity(Map("other" -> "name").asJava)
    assertThrows(classOf[UnsupportedVersionException], () => verifyDescribeEntityQuotas(entity, Map.empty))
  }

  @ClusterTest
  def testClientQuotasSanitized(): Unit = {
    // An entity with name that must be sanitized when writing to Zookeeper.
    val entity = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> "user with spaces").asJava)

    alterEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> Some(20000.0),
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0,
    ))
  }

  @ClusterTest
  def testClientQuotasWithDefaultName(): Unit = {
    // An entity using the name associated with the default entity name. The entity's name should be sanitized so
    // that it does not conflict with the default entity name.
    val entity = new ClientQuotaEntity(Map(ClientQuotaEntity.CLIENT_ID -> ZooKeeperInternals.DEFAULT_STRING).asJava)
    alterEntityQuotas(entity, Map(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> Some(20000.0)), validateOnly = false)
    verifyDescribeEntityQuotas(entity, Map(QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0))

    // This should not match.
    val result = describeClientQuotas(
      ClientQuotaFilter.containsOnly(List(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.CLIENT_ID)).asJava))
    assert(result.isEmpty)
  }

  private def verifyDescribeEntityQuotas(entity: ClientQuotaEntity, quotas: Map[String, Double]): Unit = {
    TestUtils.tryUntilNoAssertionError(waitTime = 5000L) {
      val components = entity.entries.asScala.map { case (entityType, entityName) =>
        Option(entityName).map{ name => ClientQuotaFilterComponent.ofEntity(entityType, name)}
          .getOrElse(ClientQuotaFilterComponent.ofDefaultEntity(entityType)
          )
      }
      val describe = describeClientQuotas(ClientQuotaFilter.containsOnly(components.toList.asJava))
      if (quotas.isEmpty) {
        assertEquals(0, describe.size)
      } else {
        assertEquals(1, describe.size)
        val configs = describe.get(entity)
        assertNotNull(configs)
        assertEquals(quotas.size, configs.size)
        quotas.foreach { case (k, v) =>
          val value = configs.get(k)
          assertNotNull(value)
          assertEquals(v, value, 1e-6)
        }
      }
    }
  }

  private def toClientEntity(user: Option[String], clientId: Option[String]) =
    new ClientQuotaEntity((user.map(ClientQuotaEntity.USER -> _) ++ clientId.map(ClientQuotaEntity.CLIENT_ID -> _)).toMap.asJava)

  private def toIpEntity(ip: Option[String]) = new ClientQuotaEntity(ip.map(ClientQuotaEntity.IP -> _).toMap.asJava)

  private def describeClientQuotas(filter: ClientQuotaFilter) = {
    val result = new KafkaFutureImpl[java.util.Map[ClientQuotaEntity, java.util.Map[String, java.lang.Double]]]
    sendDescribeClientQuotasRequest(filter).complete(result)
    try result.get catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private def sendDescribeClientQuotasRequest(filter: ClientQuotaFilter): DescribeClientQuotasResponse = {
    val request = new DescribeClientQuotasRequest.Builder(filter).build()
    IntegrationTestUtils.connectAndReceive[DescribeClientQuotasResponse](request,
      destination = cluster.anyBrokerSocketServer(),
      listenerName = cluster.clientListener())
  }

  private def alterEntityQuotas(entity: ClientQuotaEntity, alter: Map[String, Option[Double]], validateOnly: Boolean) =
    try alterClientQuotas(Map(entity -> alter), validateOnly)(entity).get(10, TimeUnit.SECONDS) catch {
      case e: ExecutionException => throw e.getCause
    }

  private def alterClientQuotas(request: Map[ClientQuotaEntity, Map[String, Option[Double]]], validateOnly: Boolean) = {
    val entries = request.map { case (entity, alter) =>
      val ops = alter.map { case (key, value) =>
        new ClientQuotaAlteration.Op(key, value.map(Double.box).orNull)
      }.asJavaCollection
      new ClientQuotaAlteration(entity, ops)
    }

    val response = request.map(e => e._1 -> new KafkaFutureImpl[Void]).asJava
    sendAlterClientQuotasRequest(entries, validateOnly).complete(response)
    val result = response.asScala
    assertEquals(request.size, result.size)
    request.foreach(e => assertTrue(result.contains(e._1)))
    result
  }

  private def sendAlterClientQuotasRequest(entries: Iterable[ClientQuotaAlteration], validateOnly: Boolean): AlterClientQuotasResponse = {
    val request = new AlterClientQuotasRequest.Builder(entries.asJavaCollection, validateOnly).build()
    IntegrationTestUtils.connectAndReceive[AlterClientQuotasResponse](request,
      destination = cluster.anyBrokerSocketServer(),
      listenerName = cluster.clientListener())
  }

}

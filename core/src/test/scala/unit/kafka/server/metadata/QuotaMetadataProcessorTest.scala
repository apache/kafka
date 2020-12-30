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

package kafka.server.metadata

import kafka.network.ConnectionQuotas
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.{ConfigEntityName, KafkaConfig, QuotaFactory}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.errors.{InvalidRequestException, UnsupportedVersionException}
import org.apache.kafka.common.metadata.QuotaRecord
import org.apache.kafka.common.metrics.{Metrics, Quota}
import org.apache.kafka.common.quota.{ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.junit.Assert.{assertEquals, assertFalse, assertThrows}
import org.junit.{Before, Test}
import org.mockito.ArgumentMatchers.{any, eq => _eq}
import org.mockito.Mockito._

import java.net.InetAddress
import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class QuotaMetadataProcessorTest {

  var processor: QuotaMetadataProcessor = _
  var cache: QuotaCache = _

  @Before
  def setup(): Unit = {
    val configs = TestUtils.createBrokerConfigs(1, TestUtils.MockZkConnect)
      .map(KafkaConfig.fromProps(_, new Properties()))

    val time = new MockTime
    val metrics = new Metrics
    val quotaManagers = QuotaFactory.instantiate(configs.head, metrics, time, "quota-metadata-processor-test")
    val spiedQuotaManagers = QuotaManagers(
      fetch = spy(quotaManagers.fetch),
      produce = spy(quotaManagers.produce),
      request = spy(quotaManagers.request),
      controllerMutation = spy(quotaManagers.controllerMutation),
      leader = quotaManagers.leader,
      follower = quotaManagers.follower,
      alterLogDirs = quotaManagers.alterLogDirs,
      clientQuotaCallback = quotaManagers.clientQuotaCallback
    )
    val connectionQuotas = mock(classOf[ConnectionQuotas])
    cache = new QuotaCache()
    processor = new QuotaMetadataProcessor(spiedQuotaManagers, connectionQuotas, cache)
  }

  @Test
  def testDescribeStrictMatch(): Unit = {
    setupAndVerify(processor, { case (entity, _) =>
      val components = mutable.ListBuffer[ClientQuotaFilterComponent]()
      entityToFilter(entity, components)
      val filter = ClientQuotaFilter.containsOnly(components.toList.asJava)
      val results = cache.describeClientQuotas(filter)
      assertEquals(s"Should only match one quota for ${entity}", 1, results.size)
    })

    val nonMatching = List(
      userClientEntity("user-1", "client-id-2"),
      userClientEntity("user-3", "client-id-1"),
      userClientEntity("user-2", null),
      userEntity("user-4"),
      userClientEntity(null, "client-id-2"),
      clientEntity("client-id-1"),
      clientEntity("client-id-3")
    )

    nonMatching.foreach( entity => {
      val components = mutable.ListBuffer[ClientQuotaFilterComponent]()
      entityToFilter(entity, components)
      val filter = ClientQuotaFilter.containsOnly(components.toList.asJava)
      val results = cache.describeClientQuotas(filter)
      assertEquals(0, results.size)
    })
  }

  @Test
  def testDescribeNonStrictMatch(): Unit = {
    setupAndVerify(processor, { case (_, _) => })

    // Match open-ended existing user.
    val components = mutable.ListBuffer[ClientQuotaFilterComponent]()
    entityToFilter(userEntity("user-1"), components)
    var results = cache.describeClientQuotas(ClientQuotaFilter.contains(components.toList.asJava))
    assertEquals(3, results.size)
    assertEquals(3, results.keySet.count(quotaEntity => quotaEntity match {
      case UserEntity(user) => user.equals("user-1")
      case UserDefaultClientIdEntity(user) => user.equals("user-1")
      case UserClientIdEntity(user, _) => user.equals("user-1")
      case _ => false
    }))

    results = cache.describeClientQuotas(ClientQuotaFilter.containsOnly(components.toList.asJava))
    assertEquals(1, results.size)

    // Match open-ended non-existent user.
    components.clear()
    entityToFilter(userEntity("unknown"), components)
    results = cache.describeClientQuotas(ClientQuotaFilter.contains(components.toList.asJava))
    assertEquals(0, results.size)

    // Match open-ended existing client ID.
    components.clear()
    entityToFilter(clientEntity("client-id-2"), components)
    results = cache.describeClientQuotas(ClientQuotaFilter.contains(components.toList.asJava))
    assertEquals(2, results.size)
    assertEquals(2, results.keySet.count(quotaEntity => quotaEntity match {
      case ClientIdEntity(clientId) => clientId.equals("client-id-2")
      case DefaultUserClientIdEntity(clientId) => clientId.equals("client-id-2")
      case UserClientIdEntity(_, clientId) => clientId.equals("client-id-2")
      case _ => false
    }))

    // Match open-ended default user.
    results = cache.describeClientQuotas(
      ClientQuotaFilter.contains(List(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER)).asJava))
    assertEquals(3, results.size)
    assertEquals(3, results.keySet.count(quotaEntity => quotaEntity match {
      case DefaultUserEntity | DefaultUserClientIdEntity(_) | DefaultUserDefaultClientIdEntity => true
      case _ => false
    }))

    // Match open-ended default client.
    results = cache.describeClientQuotas(
      ClientQuotaFilter.contains(List(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.CLIENT_ID)).asJava))
    assertEquals(3, results.size)
    assertEquals(3, results.keySet.count(quotaEntity => quotaEntity match {
      case DefaultClientIdEntity | UserDefaultClientIdEntity(_) | DefaultUserDefaultClientIdEntity => true
      case _ => false
    }))
  }

  @Test
  def testDescribeFilterOnTypes(): Unit = {
    setupAndVerify(processor, { case (_, _) => })

    var results = cache.describeClientQuotas(
      ClientQuotaFilter.contains(List(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER)).asJava))
    assertEquals(11, results.size)
    assertEquals(11, results.keySet.count(quotaEntity => quotaEntity match {
      case UserEntity(_) | DefaultUserEntity | UserClientIdEntity(_, _) | UserDefaultClientIdEntity(_) |
           DefaultUserClientIdEntity(_) | DefaultUserDefaultClientIdEntity => true
      case _ => false
    }))

    results = cache.describeClientQuotas(
      ClientQuotaFilter.contains(List(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.CLIENT_ID)).asJava))
    assertEquals(8, results.size)
    assertEquals(8, results.keySet.count(quotaEntity => quotaEntity match {
      case ClientIdEntity(_) | DefaultClientIdEntity | UserClientIdEntity(_, _) | UserDefaultClientIdEntity(_) |
           DefaultUserClientIdEntity(_) | DefaultUserDefaultClientIdEntity => true
      case _ => false
    }))

    results = cache.describeClientQuotas(
      ClientQuotaFilter.containsOnly(List(
        ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER),
        ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.CLIENT_ID)
      ).asJava))
    assertEquals(7, results.size)
    assertEquals(7, results.keySet.count(quotaEntity => quotaEntity match {
      case UserClientIdEntity(_, _) | UserDefaultClientIdEntity(_) |
           DefaultUserClientIdEntity(_) | DefaultUserDefaultClientIdEntity => true
      case _ => false
    }))
  }

  @Test
  def testEntityWithDefaultName(): Unit = {
    addQuotaRecord(processor, clientEntity(ConfigEntityName.Default), (QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0))
    addQuotaRecord(processor, clientEntity(null), (QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 30000.0))

    val components = mutable.ListBuffer[ClientQuotaFilterComponent]()
    entityToFilter(clientEntity(ConfigEntityName.Default), components)
    var results = cache.describeClientQuotas(ClientQuotaFilter.containsOnly(components.toList.asJava))
    assertEquals(1, results.size)

    components.clear()
    entityToFilter(clientEntity(null), components)
    results = cache.describeClientQuotas(ClientQuotaFilter.containsOnly(components.toList.asJava))
    assertEquals(1, results.size)
  }

  @Test
  def testQuotaRemoval(): Unit = {
    val entity = userClientEntity("user", "client-id")
    addQuotaRecord(processor, entity, (QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 10000.0))
    addQuotaRecord(processor, entity, (QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0))
    var quotas = describeEntity(entity)
    assertEquals(2, quotas.size)
    assertEquals(10000.0, quotas(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG), 1e-6)

    addQuotaRecord(processor, entity, (QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 10001.0))
    quotas = describeEntity(entity)
    assertEquals(2, quotas.size)
    assertEquals(10001.0, quotas(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG), 1e-6)

    addQuotaRemovalRecord(processor, entity, QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG)
    quotas = describeEntity(entity)
    assertEquals(1, quotas.size)
    assertFalse(quotas.contains(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG))

    addQuotaRemovalRecord(processor, entity, QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG)
    quotas = describeEntity(entity)
    assertEquals(0, quotas.size)

    // Removing non-existent quota should not do anything
    addQuotaRemovalRecord(processor, entity, QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG)
    quotas = describeEntity(entity)
    assertEquals(0, quotas.size)
  }

  @Test
  def testDescribeClientQuotasInvalidFilterCombination(): Unit = {
    val ipFilterComponent = ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.IP)
    val userFilterComponent = ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER)
    val clientIdFilterComponent = ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.CLIENT_ID)
    val expectedExceptionMessage = "Invalid entity filter component combination"
    assertThrows(expectedExceptionMessage, classOf[InvalidRequestException],
      () => cache.describeClientQuotas(ClientQuotaFilter.contains(List(ipFilterComponent, userFilterComponent).asJava)))
    assertThrows(expectedExceptionMessage, classOf[InvalidRequestException],
      () => cache.describeClientQuotas(ClientQuotaFilter.contains(List(ipFilterComponent, clientIdFilterComponent).asJava)))
    assertThrows(expectedExceptionMessage, classOf[InvalidRequestException],
      () => cache.describeClientQuotas(ClientQuotaFilter.contains(List(ipFilterComponent, ipFilterComponent).asJava)))
    assertThrows(expectedExceptionMessage, classOf[InvalidRequestException],
      () => cache.describeClientQuotas(ClientQuotaFilter.contains(List(userFilterComponent, userFilterComponent).asJava)))
  }

  @Test
  def testDescribeEmptyFilter(): Unit = {
    var results = cache.describeClientQuotas(ClientQuotaFilter.contains(List.empty.asJava))
    assertEquals(0, results.size)

    results = cache.describeClientQuotas(ClientQuotaFilter.containsOnly(List.empty.asJava))
    assertEquals(0, results.size)
  }

  @Test
  def testDescribeUnsupportedEntityType(): Unit = {
    val filter = ClientQuotaFilter.contains(
      List(ClientQuotaFilterComponent.ofEntityType("other")).asJava)
    assertThrows(classOf[UnsupportedVersionException],
      () => cache.describeClientQuotas(filter))
  }

  @Test
  def testDescribeMissingEntityType(): Unit = {
    val filter = ClientQuotaFilter.contains(
      List(ClientQuotaFilterComponent.ofEntity("", "name")).asJava)
    assertThrows(classOf[InvalidRequestException],
      () => cache.describeClientQuotas(filter))
  }

  @Test
  def testQuotaManagers(): Unit = {
    val entity = userClientEntity("user", "client")
    addQuotaRecord(processor, entity, (QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 100.0))
    verify(processor.quotaManagers.fetch, times(1)).updateQuota(
      _eq(Some("user")),
      _eq(Some("client")),
      _eq(Some("client")),
      any(classOf[Option[Quota]])
    )

    addQuotaRecord(processor, entity, (QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 100.0))
    verify(processor.quotaManagers.produce, times(1)).updateQuota(
      _eq(Some("user")),
      _eq(Some("client")),
      _eq(Some("client")),
      any(classOf[Option[Quota]])
    )

    addQuotaRecord(processor, entity, (QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 100.0))
    verify(processor.quotaManagers.request, times(1)).updateQuota(
      _eq(Some("user")),
      _eq(Some("client")),
      _eq(Some("client")),
      any(classOf[Option[Quota]])
    )

    addQuotaRecord(processor, entity, (QuotaConfigs.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG, 100.0))
    verify(processor.quotaManagers.controllerMutation, times(1)).updateQuota(
      _eq(Some("user")),
      _eq(Some("client")),
      _eq(Some("client")),
      any(classOf[Option[Quota]])
    )

    addQuotaRemovalRecord(processor, entity, QuotaConfigs.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG)
    verify(processor.quotaManagers.controllerMutation, times(1)).updateQuota(
      _eq(Some("user")),
      _eq(Some("client")),
      _eq(Some("client")),
      _eq(None)
    )
  }

  @Test
  def testIpQuota(): Unit = {
    val defaultIp = ipEntity(null)
    val knownIp = ipEntity("1.2.3.4")

    addQuotaRecord(processor, defaultIp, (QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, 100.0))
    addQuotaRecord(processor, knownIp, (QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, 99.0))

    verify(processor.connectionQuotas, times(2)).updateIpConnectionRateQuota(
      any(classOf[Option[InetAddress]]),
      any(classOf[Option[Int]])
    )

    var quotas = describeEntity(defaultIp)
    assertEquals(1, quotas.size)

    quotas = describeEntity(knownIp)
    assertEquals(1, quotas.size)

    val filter = ClientQuotaFilter.contains(
      List(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.IP)).asJava)
    val results = cache.describeClientQuotas(filter)
    assertEquals(2, results.size)

    reset(processor.connectionQuotas)
    addQuotaRecord(processor, knownIp, (QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, 98.0))
    verify(processor.connectionQuotas, times(1)).updateIpConnectionRateQuota(
      any(classOf[Option[InetAddress]]),
      _eq(Some(98))
    )

    reset(processor.connectionQuotas)
    addQuotaRemovalRecord(processor, knownIp, QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG)
    verify(processor.connectionQuotas, times(1)).updateIpConnectionRateQuota(
      any(classOf[Option[InetAddress]]),
      _eq(None)
    )
  }

  @Test
  def testIpQuotaUnknownKey(): Unit = {
    val defaultIp = ipEntity(null)
    addQuotaRecord(processor, defaultIp, ("not-an-ip-quota-key", 100.0))
    verify(processor.connectionQuotas, times(0)).updateIpConnectionRateQuota(
      any(classOf[Option[InetAddress]]),
      _eq(Some(100))
    )

    assertEquals(0, describeEntity(defaultIp).size)
  }

  @Test
  def testUserQuotaUnknownKey(): Unit = {
    val defaultUser = userEntity(null)
    addQuotaRecord(processor, defaultUser, ("not-a-user-quota-key", 100.0))
    assertEquals(0, describeEntity(defaultUser).size)
  }

  def setupAndVerify(processor: QuotaMetadataProcessor,
                     verifier: (List[QuotaRecord.EntityData], (String, Double)) => Unit ): Unit = {
    val toVerify = List(
      (userClientEntity("user-1", "client-id-1"), 50.50),
      (userClientEntity("user-2", "client-id-1"), 51.51),
      (userClientEntity("user-3", "client-id-2"), 52.52),
      (userClientEntity(null, "client-id-1"), 53.53),
      (userClientEntity("user-1", null), 54.54),
      (userClientEntity("user-3", null), 55.55),
      (userEntity("user-1"), 56.56),
      (userEntity("user-2"), 57.57),
      (userEntity("user-3"), 58.58),
      (userEntity(null), 59.59),
      (clientEntity("client-id-2"), 60.60),
      (userClientEntity(null, null), 61.61)
    )

    toVerify.foreach {
      case (entity, value) => addQuotaRecord(processor, entity, (QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, value))
    }

    toVerify.foreach {
      case (entity, value) => verifier.apply(entity, (QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, value))
    }
  }

  def describeEntity(entity: List[QuotaRecord.EntityData]): Map[String, Double] = {
    val components = mutable.ListBuffer[ClientQuotaFilterComponent]()
    entityToFilter(entity, components)
    val results = cache.describeClientQuotas(ClientQuotaFilter.containsOnly(components.toList.asJava))
    if (results.isEmpty) {
      Map()
    } else if (results.size == 1) {
      results.head._2
    } else {
      throw new AssertionError("Matched more than one entity with strict=true describe filter")
    }
  }

  def addQuotaRecord(processor: QuotaMetadataProcessor, entity: List[QuotaRecord.EntityData], quota: (String, Double)): Unit = {
    processor.handleQuotaRecord(new QuotaRecord()
      .setEntity(entity.asJava)
      .setKey(quota._1)
      .setValue(quota._2))
  }

  def addQuotaRemovalRecord(processor: QuotaMetadataProcessor, entity: List[QuotaRecord.EntityData], quota: String): Unit = {
    processor.handleQuotaRecord(new QuotaRecord()
      .setEntity(entity.asJava)
      .setKey(quota)
      .setRemove(true))
  }

  def entityToFilter(entity: List[QuotaRecord.EntityData], components: mutable.ListBuffer[ClientQuotaFilterComponent]): Unit = {
    entity.foreach(entityData => {
      if (entityData.entityName() == null) {
        components.append(ClientQuotaFilterComponent.ofDefaultEntity(entityData.entityType()))
      } else {
        components.append(ClientQuotaFilterComponent.ofEntity(entityData.entityType(), entityData.entityName()))
      }
    })
  }

  def clientEntity(clientId: String): List[QuotaRecord.EntityData] = {
    List(new QuotaRecord.EntityData().setEntityType(ClientQuotaEntity.CLIENT_ID).setEntityName(clientId))
  }

  def userEntity(user: String): List[QuotaRecord.EntityData] = {
    List(new QuotaRecord.EntityData().setEntityType(ClientQuotaEntity.USER).setEntityName(user))
  }

  def userClientEntity(user: String, clientId: String): List[QuotaRecord.EntityData] = {
    List(
      new QuotaRecord.EntityData().setEntityType(ClientQuotaEntity.USER).setEntityName(user),
      new QuotaRecord.EntityData().setEntityType(ClientQuotaEntity.CLIENT_ID).setEntityName(clientId)
    )
  }

  def ipEntity(ip: String): List[QuotaRecord.EntityData] = {
    List(new QuotaRecord.EntityData().setEntityType(ClientQuotaEntity.IP).setEntityName(ip))
  }
}

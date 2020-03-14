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

import org.apache.kafka.common.errors.{InvalidRequestException, UnsupportedVersionException}
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.common.requests.{AlterClientQuotasRequest, AlterClientQuotasResponse, DescribeClientQuotasRequest, DescribeClientQuotasResponse}
import org.junit.Assert._
import org.junit.Test

import java.util.concurrent.{ExecutionException, TimeUnit}

import scala.collection.JavaConverters._

class ClientQuotasRequestTest extends BaseRequestTest {
  private val ConsumerByteRateProp = DynamicConfig.Client.ConsumerByteRateOverrideProp
  private val ProducerByteRateProp = DynamicConfig.Client.ProducerByteRateOverrideProp
  private val RequestPercentageProp = DynamicConfig.Client.RequestPercentageOverrideProp

  override val brokerCount = 1

  @Test
  def testAlterClientQuotasRequest(): Unit = {
    val entity = new ClientQuotaEntity(Map((ClientQuotaEntity.USER -> "user"), (ClientQuotaEntity.CLIENT_ID -> "client-id")).asJava)

    // Expect an empty configuration.
    verifyDescribeEntityQuotas(entity, Map.empty)

    // Add two configuration entries.
    alterEntityQuotas(entity, Map(
      (ProducerByteRateProp -> Some(10000.0)),
      (ConsumerByteRateProp -> Some(20000.0))
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      (ProducerByteRateProp -> 10000.0),
      (ConsumerByteRateProp -> 20000.0)
    ))

    // Update an existing entry.
    alterEntityQuotas(entity, Map(
      (ProducerByteRateProp -> Some(15000.0))
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      (ProducerByteRateProp -> 15000.0),
      (ConsumerByteRateProp -> 20000.0)
    ))

    // Remove an existing configuration entry.
    alterEntityQuotas(entity, Map(
      (ProducerByteRateProp -> None)
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      (ConsumerByteRateProp -> 20000.0)
    ))

    // Remove a non-existent configuration entry.  This should make no changes.
    alterEntityQuotas(entity, Map(
      (RequestPercentageProp -> None)
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      (ConsumerByteRateProp -> 20000.0)
    ))

    // Add back a deleted configuration entry.
    alterEntityQuotas(entity, Map(
      (ProducerByteRateProp -> Some(5000.0))
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      (ProducerByteRateProp -> 5000.0),
      (ConsumerByteRateProp -> 20000.0)
    ))

    // Perform a mixed update.
    alterEntityQuotas(entity, Map(
      (ProducerByteRateProp -> Some(20000.0)),
      (ConsumerByteRateProp -> None),
      (RequestPercentageProp -> Some(12.3))
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      (ProducerByteRateProp -> 20000.0),
      (RequestPercentageProp -> 12.3)
    ))
  }

  @Test
  def testAlterClientQuotasRequestValidateOnly(): Unit = {
    val entity = new ClientQuotaEntity(Map((ClientQuotaEntity.USER -> "user")).asJava)

    // Set up a configuration.
    alterEntityQuotas(entity, Map(
      (ProducerByteRateProp -> Some(20000.0)),
      (RequestPercentageProp -> Some(23.45))
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      (ProducerByteRateProp -> 20000.0),
      (RequestPercentageProp -> 23.45)
    ))

    // Validate-only addition.
    alterEntityQuotas(entity, Map(
      (ConsumerByteRateProp -> Some(50000.0))
    ), validateOnly = true)

    verifyDescribeEntityQuotas(entity, Map(
      (ProducerByteRateProp -> 20000.0),
      (RequestPercentageProp -> 23.45)
    ))

    // Validate-only modification.
    alterEntityQuotas(entity, Map(
      (ProducerByteRateProp -> Some(10000.0))
    ), validateOnly = true)

    verifyDescribeEntityQuotas(entity, Map(
      (ProducerByteRateProp -> 20000.0),
      (RequestPercentageProp -> 23.45)
    ))

    // Validate-only removal.
    alterEntityQuotas(entity, Map(
      (RequestPercentageProp -> None)
    ), validateOnly = true)

    verifyDescribeEntityQuotas(entity, Map(
      (ProducerByteRateProp -> 20000.0),
      (RequestPercentageProp -> 23.45)
    ))

    // Validate-only mixed update.
    alterEntityQuotas(entity, Map(
      (ProducerByteRateProp -> Some(10000.0)),
      (ConsumerByteRateProp -> Some(50000.0)),
      (RequestPercentageProp -> None)
    ), validateOnly = true)

    verifyDescribeEntityQuotas(entity, Map(
      (ProducerByteRateProp -> 20000.0),
      (RequestPercentageProp -> 23.45)
    ))
  }

  @Test(expected = classOf[InvalidRequestException])
  def testAlterClientQuotasBadUser(): Unit = {
    val entity = new ClientQuotaEntity(Map((ClientQuotaEntity.USER -> "")).asJava)
    alterEntityQuotas(entity, Map((RequestPercentageProp -> Some(12.34))), validateOnly = true)
  }

  @Test(expected = classOf[InvalidRequestException])
  def testAlterClientQuotasBadClientId(): Unit = {
    val entity = new ClientQuotaEntity(Map((ClientQuotaEntity.CLIENT_ID -> "")).asJava)
    alterEntityQuotas(entity, Map((RequestPercentageProp -> Some(12.34))), validateOnly = true)
  }

  @Test(expected = classOf[InvalidRequestException])
  def testAlterClientQuotasBadEntityType(): Unit = {
    val entity = new ClientQuotaEntity(Map(("" -> "name")).asJava)
    alterEntityQuotas(entity, Map((RequestPercentageProp -> Some(12.34))), validateOnly = true)
  }

  @Test(expected = classOf[InvalidRequestException])
  def testAlterClientQuotasEmptyEntity(): Unit = {
    val entity = new ClientQuotaEntity(Map.empty.asJava)
    alterEntityQuotas(entity, Map((ProducerByteRateProp -> Some(10000.5))), validateOnly = true)
  }

  @Test(expected = classOf[InvalidRequestException])
  def testAlterClientQuotasBadConfigKey(): Unit = {
    val entity = new ClientQuotaEntity(Map((ClientQuotaEntity.USER -> "user")).asJava)
    alterEntityQuotas(entity, Map(("bad" -> Some(1.0))), validateOnly = true)
  }

  @Test(expected = classOf[InvalidRequestException])
  def testAlterClientQuotasBadConfigValue(): Unit = {
    val entity = new ClientQuotaEntity(Map((ClientQuotaEntity.USER -> "user")).asJava)
    alterEntityQuotas(entity, Map((ProducerByteRateProp -> Some(10000.5))), validateOnly = true)
  }

  // Entities to be matched against.
  private val matchEntities = List(
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
  ).map { case (u, c, v) => (toEntity(u, c), v) }

  private def setupDescribeClientQuotasMatchTest() = {
    val result = alterClientQuotas(matchEntities.map { case (e, v) =>
      (e -> Map((RequestPercentageProp, Some(v))))
    }.toMap, validateOnly = false)
    matchEntities.foreach(e => result.get(e._1).get.get(10, TimeUnit.SECONDS))

    // Allow time for watch callbacks to be triggered.
    Thread.sleep(500)
  }

  @Test
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
    matchEntities.foreach { case (e, v) =>
      val result = matchEntity(e)
      assertEquals(1, result.size)
      assertTrue(result.get(e) != null)
      val value = result.get(e).get(RequestPercentageProp)
      assertTrue(value != null)
      assertEquals(value, v, 1e-6)
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

  @Test
  def testDescribeClientQuotasMatchPartial(): Unit = {
    setupDescribeClientQuotasMatchTest()

    def testMatchEntities(filter: ClientQuotaFilter, expectedMatchSize: Int, partition: ClientQuotaEntity => Boolean) {
      val result = describeClientQuotas(filter)
      val (expectedMatches, expectedNonMatches) = matchEntities.partition(e => partition(e._1))
      assertEquals(expectedMatchSize, expectedMatches.size)  // for test verification
      assertEquals(expectedMatchSize, result.size)
      val expectedMatchesMap = expectedMatches.toMap
      matchEntities.foreach { case (entity, expectedValue) =>
        if (expectedMatchesMap.contains(entity)) {
          val config = result.get(entity)
          assertTrue(config != null)
          val value = config.get(RequestPercentageProp)
          assertTrue(value != null)
          assertEquals(expectedValue, value, 1e-6)
        } else {
          assertTrue(result.get(entity) == null)
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

    // Match open-ended empty filter list. This should match all entities.
    testMatchEntities(ClientQuotaFilter.contains(List.empty.asJava), 11, entity => true)

    // Match close-ended empty filter list. This should match no entities.
    testMatchEntities(ClientQuotaFilter.containsOnly(List.empty.asJava), 0, entity => false)
  }

  @Test
  def testClientQuotasUnsupportedEntityTypes() {
    val entity = new ClientQuotaEntity(Map(("other" -> "name")).asJava)
    try {
      verifyDescribeEntityQuotas(entity, Map())
    } catch {
      case e: ExecutionException => assertTrue(e.getCause.isInstanceOf[UnsupportedVersionException])
    }
  }

  @Test
  def testClientQuotasSanitized(): Unit = {
    // An entity with name that must be sanitized when writing to Zookeeper.
    val entity = new ClientQuotaEntity(Map((ClientQuotaEntity.USER -> "user with spaces")).asJava)

    alterEntityQuotas(entity, Map(
      (ProducerByteRateProp -> Some(20000.0)),
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      (ProducerByteRateProp -> 20000.0),
    ))
  }

  private def verifyDescribeEntityQuotas(entity: ClientQuotaEntity, quotas: Map[String, Double]) = {
    val components = entity.entries.asScala.map(e => ClientQuotaFilterComponent.ofEntity(e._1, e._2))
    val describe = describeClientQuotas(ClientQuotaFilter.containsOnly(components.toList.asJava))
    if (quotas.isEmpty) {
      assertEquals(0, describe.size)
    } else {
      assertEquals(1, describe.size)
      val configs = describe.get(entity)
      assertTrue(configs != null)
      assertEquals(quotas.size, configs.size)
      quotas.foreach { case (k, v) =>
        val value = configs.get(k)
        assertTrue(value != null)
        assertEquals(v, value, 1e-6)
      }
    }
  }

  private def toEntity(user: Option[String], clientId: Option[String]) =
    new ClientQuotaEntity((user.map((ClientQuotaEntity.USER -> _)) ++ clientId.map((ClientQuotaEntity.CLIENT_ID -> _))).toMap.asJava)

  private def describeClientQuotas(filter: ClientQuotaFilter) = {
    val result = new KafkaFutureImpl[java.util.Map[ClientQuotaEntity, java.util.Map[String, java.lang.Double]]]
    sendDescribeClientQuotasRequest(filter).complete(result)
    result.get
  }

  private def sendDescribeClientQuotasRequest(filter: ClientQuotaFilter): DescribeClientQuotasResponse = {
    val request = new DescribeClientQuotasRequest.Builder(filter).build()
    connectAndReceive[DescribeClientQuotasResponse](request, destination = controllerSocketServer)
  }

  private def alterEntityQuotas(entity: ClientQuotaEntity, alter: Map[String, Option[Double]], validateOnly: Boolean) =
    try alterClientQuotas(Map(entity -> alter), validateOnly).get(entity).get.get(10, TimeUnit.SECONDS) catch {
      case e: ExecutionException => throw e.getCause
    }

  private def alterClientQuotas(request: Map[ClientQuotaEntity, Map[String, Option[Double]]], validateOnly: Boolean) = {
    val entries = request.map { case (entity, alter) =>
      val ops = alter.map { case (key, value) =>
        new ClientQuotaAlteration.Op(key, value.map(Double.box).getOrElse(null))
      }.asJavaCollection
      new ClientQuotaAlteration(entity, ops)
    }

    val response = request.map(e => (e._1 -> new KafkaFutureImpl[Void])).asJava
    sendAlterClientQuotasRequest(entries, validateOnly).complete(response)
    val result = response.asScala
    assertEquals(request.size, result.size)
    request.foreach(e => assertTrue(result.get(e._1).isDefined))
    result
  }

  private def sendAlterClientQuotasRequest(entries: Iterable[ClientQuotaAlteration], validateOnly: Boolean): AlterClientQuotasResponse = {
    val request = new AlterClientQuotasRequest.Builder(entries.asJavaCollection, validateOnly).build()
    connectAndReceive[AlterClientQuotasResponse](request, destination = controllerSocketServer)
  }

}

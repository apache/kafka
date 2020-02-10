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

import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.quota.{QuotaAlteration, QuotaEntity, QuotaFilter}
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
    val entity = new QuotaEntity(Map((QuotaEntity.USER -> "user"), (QuotaEntity.CLIENT_ID -> "client-id")).asJava)

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
    val entity = new QuotaEntity(Map((QuotaEntity.USER -> "user")).asJava)

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
    val entity = new QuotaEntity(Map((QuotaEntity.USER -> "")).asJava)
    alterEntityQuotas(entity, Map((RequestPercentageProp -> Some(12.34))), validateOnly = true)
  }

  @Test(expected = classOf[InvalidRequestException])
  def testAlterClientQuotasBadClientId(): Unit = {
    val entity = new QuotaEntity(Map((QuotaEntity.CLIENT_ID -> "")).asJava)
    alterEntityQuotas(entity, Map((RequestPercentageProp -> Some(12.34))), validateOnly = true)
  }

  @Test(expected = classOf[InvalidRequestException])
  def testAlterClientQuotasBadEntityType(): Unit = {
    val entity = new QuotaEntity(Map(("" -> "name")).asJava)
    alterEntityQuotas(entity, Map((RequestPercentageProp -> Some(12.34))), validateOnly = true)
  }

  @Test(expected = classOf[InvalidRequestException])
  def testAlterClientQuotasEmptyEntity(): Unit = {
    val entity = new QuotaEntity(Map.empty.asJava)
    alterEntityQuotas(entity, Map((ProducerByteRateProp -> Some(10000.5))), validateOnly = true)
  }

  @Test(expected = classOf[InvalidRequestException])
  def testAlterClientQuotasBadConfigKey(): Unit = {
    val entity = new QuotaEntity(Map((QuotaEntity.USER -> "user")).asJava)
    alterEntityQuotas(entity, Map(("bad" -> Some(1.0))), validateOnly = true)
  }

  @Test(expected = classOf[InvalidRequestException])
  def testAlterClientQuotasBadConfigValue(): Unit = {
    val entity = new QuotaEntity(Map((QuotaEntity.USER -> "user")).asJava)
    alterEntityQuotas(entity, Map((ProducerByteRateProp -> Some(10000.5))), validateOnly = true)
  }

  // Entities to be matched against.
  private val matchEntities = List(
    (Some("user-1"), Some("client-id-1"), 50.50),
    (Some("user-2"), Some("client-id-1"), 51.51),
    (Some("user-3"), Some("client-id-2"), 52.52),
    (Some(QuotaEntity.USER_DEFAULT), Some("client-id-1"), 53.53),
    (Some("user-1"), Some(QuotaEntity.CLIENT_ID_DEFAULT), 54.54),
    (Some("user-3"), Some(QuotaEntity.CLIENT_ID_DEFAULT), 55.55),
    (Some("user-1"), None, 56.56),
    (Some("user-2"), None, 57.57),
    (Some("user-3"), None, 58.58),
    (Some(QuotaEntity.USER_DEFAULT), None, 59.59),
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

    def matchEntity(entity: QuotaEntity) = {
      val filters = entity.entries.asScala.map(e => QuotaFilter.matchExact(e._1, e._2))
      describeClientQuotas(filters, includeUnspecifiedTypes = false)
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
      (Some("user-1"), Some("client-id-1"), Some("user-1")),
      (Some("user-1"), Some("client-id-2"), None),
      (Some("user-3"), Some("client-id-1"), None),
      (Some("user-2"), Some(QuotaEntity.CLIENT_ID_DEFAULT), None),
      (Some("user-4"), None, None),
      (Some(QuotaEntity.USER_DEFAULT), Some("client-id-2"), None),
      (None, Some("client-id-1"), None),
      (None, Some("client-id-3"), None),
      (None, None, Some("user-1"))
    ).map { case (u, c, o) =>
        new QuotaEntity((u.map((QuotaEntity.USER, _)) ++
          c.map((QuotaEntity.CLIENT_ID, _)) ++
          o.map(("other", _))).toMap.asJava)
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

    def testMatchEntities(filters: Iterable[QuotaFilter], includeUnspecifiedTypes: Boolean, expectedMatchSize: Int, partition: QuotaEntity => Boolean) {
      val result = describeClientQuotas(filters, includeUnspecifiedTypes)
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
      List(QuotaFilter.matchExact(QuotaEntity.USER, "user-1")), true, 3,
      entity => entity.entries.get(QuotaEntity.USER) == "user-1"
    )

    // Match open-ended non-existent user.
    testMatchEntities(
      List(QuotaFilter.matchExact(QuotaEntity.USER, "unknown")), true, 0,
      entity => false
    )

    // Match open-ended existing client ID.
    testMatchEntities(
      List(QuotaFilter.matchExact(QuotaEntity.CLIENT_ID, "client-id-2")), true, 2,
      entity => entity.entries.get(QuotaEntity.CLIENT_ID) == "client-id-2"
    )

    // Match open-ended default user.
    testMatchEntities(
      List(QuotaFilter.matchExact(QuotaEntity.USER, QuotaEntity.USER_DEFAULT)), true, 2,
      entity => entity.entries.get(QuotaEntity.USER) == QuotaEntity.USER_DEFAULT
    )

    // Match close-ended existing user.
    testMatchEntities(
      List(QuotaFilter.matchExact(QuotaEntity.USER, "user-2")), false, 1,
      entity => entity.entries.get(QuotaEntity.USER) == "user-2" && entity.entries.get(QuotaEntity.CLIENT_ID) == null
    )

    // Match close-ended existing client ID that has no matching entity.
    testMatchEntities(
      List(QuotaFilter.matchExact(QuotaEntity.CLIENT_ID, "client-id-1")), false, 0,
      entity => false
    )

    // Match an "other" entity type. This shouldn't match anything (but also shouldn't produce an error) because support for
    // setting configurations of other entity types is not implemented yet.
    testMatchEntities(
      List(QuotaFilter.matchExact("other", "user-1")), true, 0,
      entity => false,
    )

    // Match against all entities with the user type in a close-ended match.
    testMatchEntities(
      List(QuotaFilter.matchSpecified(QuotaEntity.USER)), false, 4,
      entity => entity.entries.get(QuotaEntity.USER) != null && entity.entries.get(QuotaEntity.CLIENT_ID) == null
    )

    // Match against all entities with the user type in an open-ended match.
    testMatchEntities(
      List(QuotaFilter.matchSpecified(QuotaEntity.USER)), true, 10,
      entity => entity.entries.get(QuotaEntity.USER) != null
    )

    // Match against all entities with the client ID type in a close-ended match.
    testMatchEntities(
      List(QuotaFilter.matchSpecified(QuotaEntity.CLIENT_ID)), false, 1,
      entity => entity.entries.get(QuotaEntity.CLIENT_ID) != null && entity.entries.get(QuotaEntity.USER) == null
    )

    // Match against all entities with the client ID type in an open-ended match.
    testMatchEntities(
      List(QuotaFilter.matchSpecified(QuotaEntity.CLIENT_ID)), true, 7,
      entity => entity.entries.get(QuotaEntity.CLIENT_ID) != null
    )

    // Match against all entities with an unexpected type in an open-ended match.
    testMatchEntities(
      List(QuotaFilter.matchSpecified("other")), true, 0,
      entity => false
    )

    // Match open-ended empty filter list. This should match all entities.
    testMatchEntities(List.empty, true, 11, entity => true)

    // Match close-ended empty filter list. This should match no entities.
    testMatchEntities(List.empty, false, 0, entity => false)
  }

  @Test
  def testClientQuotasSanitized(): Unit = {
    // An entity with name that must be sanitized when writing to Zookeeper.
    val entity = new QuotaEntity(Map((QuotaEntity.USER -> "user with spaces")).asJava)

    alterEntityQuotas(entity, Map(
      (ProducerByteRateProp -> Some(20000.0)),
    ), validateOnly = false)

    verifyDescribeEntityQuotas(entity, Map(
      (ProducerByteRateProp -> 20000.0),
    ))
  }

  private def verifyDescribeEntityQuotas(entity: QuotaEntity, quotas: Map[String, Double]) = {
    val filters = entity.entries.asScala.map(e => QuotaFilter.matchExact(e._1, e._2))
    val describe = describeClientQuotas(filters, includeUnspecifiedTypes = false)
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
    new QuotaEntity((user.map((QuotaEntity.USER, _)) ++ clientId.map((QuotaEntity.CLIENT_ID, _))).toMap.asJava)

  private def describeClientQuotas(filters: Iterable[QuotaFilter], includeUnspecifiedTypes: Boolean) = {
    val result = new KafkaFutureImpl[java.util.Map[QuotaEntity, java.util.Map[String, java.lang.Double]]]
    sendDescribeClientQuotasRequest(filters, includeUnspecifiedTypes).complete(result)
    result.get
  }

  private def sendDescribeClientQuotasRequest(filters: Iterable[QuotaFilter], includeUnspecifiedTypes: Boolean): DescribeClientQuotasResponse = {
    val request = new DescribeClientQuotasRequest.Builder(filters.asJavaCollection, includeUnspecifiedTypes).build()
    connectAndReceive[DescribeClientQuotasResponse](request, destination = controllerSocketServer)
  }

  private def alterEntityQuotas(entity: QuotaEntity, alter: Map[String, Option[Double]], validateOnly: Boolean) =
    try alterClientQuotas(Map(entity -> alter), validateOnly).get(entity).get.get(10, TimeUnit.SECONDS) catch {
      case e: ExecutionException => throw e.getCause
    }

  private def alterClientQuotas(request: Map[QuotaEntity, Map[String, Option[Double]]], validateOnly: Boolean) = {
    val entries = request.map { case (entity, alter) =>
      val ops = alter.map { case (key, value) =>
        new QuotaAlteration.Op(key, value.map(Double.box).getOrElse(null))
      }.asJavaCollection
      new QuotaAlteration(entity, ops)
    }

    val response = request.map(e => (e._1 -> new KafkaFutureImpl[Void])).asJava
    sendAlterClientQuotasRequest(entries, validateOnly).complete(response)
    val result = response.asScala
    assertEquals(request.size, result.size)
    request.foreach(e => assertTrue(result.get(e._1).isDefined))
    result
  }

  private def sendAlterClientQuotasRequest(entries: Iterable[QuotaAlteration], validateOnly: Boolean): AlterClientQuotasResponse = {
    val request = new AlterClientQuotasRequest.Builder(entries.asJavaCollection, validateOnly).build()
    connectAndReceive[AlterClientQuotasResponse](request, destination = controllerSocketServer)
  }

}

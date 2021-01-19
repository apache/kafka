/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package kafka.server

import java.util.Properties
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

import kafka.server.ClientQuotaManager.DefaultTags
import kafka.utils.TestUtils
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.message.CreatePartitionsRequestData
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.DeleteTopicsRequestData
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.quota.ClientQuotaAlteration
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.requests.AlterClientQuotasRequest
import org.apache.kafka.common.requests.AlterClientQuotasResponse
import org.apache.kafka.common.requests.CreatePartitionsRequest
import org.apache.kafka.common.requests.CreatePartitionsResponse
import org.apache.kafka.common.requests.CreateTopicsRequest
import org.apache.kafka.common.requests.CreateTopicsResponse
import org.apache.kafka.common.requests.DeleteTopicsRequest
import org.apache.kafka.common.requests.DeleteTopicsResponse
import org.apache.kafka.common.security.auth.AuthenticationContext
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

object ControllerMutationQuotaTest {
  // Principal used for all client connections. This is updated by each test.
  var principal = KafkaPrincipal.ANONYMOUS
  class TestPrincipalBuilder extends KafkaPrincipalBuilder {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      principal
    }
  }

  def asPrincipal(newPrincipal: KafkaPrincipal)(f: => Unit): Unit = {
    val currentPrincipal = principal
    principal = newPrincipal
    try f
    finally principal = currentPrincipal
  }

  val ThrottledPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ThrottledPrincipal")
  val UnboundedPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "UnboundedPrincipal")

  val StrictCreateTopicsRequestVersion = ApiKeys.CREATE_TOPICS.latestVersion
  val PermissiveCreateTopicsRequestVersion = 5.toShort

  val StrictDeleteTopicsRequestVersion = ApiKeys.DELETE_TOPICS.latestVersion
  val PermissiveDeleteTopicsRequestVersion = 4.toShort

  val StrictCreatePartitionsRequestVersion = ApiKeys.CREATE_PARTITIONS.latestVersion
  val PermissiveCreatePartitionsRequestVersion = 2.toShort

  val Topic1 = "topic-1"
  val Topic2 = "topic-2"
  val TopicsWithOnePartition = Map(Topic1 ->  1, Topic2 ->  1)
  val TopicsWith30Partitions = Map(Topic1 -> 30, Topic2 -> 30)
  val TopicsWith31Partitions = Map(Topic1 -> 31, Topic2 -> 31)

  val ControllerQuotaSamples = 10
  val ControllerQuotaWindowSizeSeconds = 1
  val ControllerMutationRate = 2.0
}

class ControllerMutationQuotaTest extends BaseRequestTest {
  import ControllerMutationQuotaTest._

  override def brokerCount: Int = 1

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.ControlledShutdownEnableProp, "false")
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.PrincipalBuilderClassProp,
      classOf[ControllerMutationQuotaTest.TestPrincipalBuilder].getName)
    // Specify number of samples and window size.
    properties.put(KafkaConfig.NumControllerQuotaSamplesProp, ControllerQuotaSamples.toString)
    properties.put(KafkaConfig.ControllerQuotaWindowSizeSecondsProp, ControllerQuotaWindowSizeSeconds.toString)
  }

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()

    // Define a quota for ThrottledPrincipal
    defineUserQuota(ThrottledPrincipal.getName, Some(ControllerMutationRate))
    waitUserQuota(ThrottledPrincipal.getName, ControllerMutationRate)
  }

  @Test
  def testSetUnsetQuota(): Unit = {
    val rate = 1.5
    val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "User")
    // Default Value
    waitUserQuota(principal.getName, Long.MaxValue)
    // Define a new quota
    defineUserQuota(principal.getName, Some(rate))
    // Check it
    waitUserQuota(principal.getName, rate)
    // Remove it
    defineUserQuota(principal.getName, None)
    // Back to the default
    waitUserQuota(principal.getName, Long.MaxValue)
  }

  @Test
  def testQuotaMetric(): Unit = {
    asPrincipal(ThrottledPrincipal) {
      // Metric is lazily created
      assertTrue(quotaMetric(principal.getName).isEmpty)

      // Create a topic to create the metrics
      val (_, errors) = createTopics(Map("topic" -> 1), StrictDeleteTopicsRequestVersion)
      assertEquals(Set(Errors.NONE), errors.values.toSet)

      // Metric must be there with the correct config
      waitQuotaMetric(principal.getName, ControllerMutationRate)

      // Update quota
      defineUserQuota(ThrottledPrincipal.getName, Some(ControllerMutationRate * 2))
      waitUserQuota(ThrottledPrincipal.getName, ControllerMutationRate * 2)

      // Metric must be there with the updated config
      waitQuotaMetric(principal.getName, ControllerMutationRate * 2)
    }
  }

  @Test
  def testStrictCreateTopicsRequest(): Unit = {
    asPrincipal(ThrottledPrincipal) {
      // Create two topics worth of 30 partitions each. As we use a strict quota, we
      // expect one to be created and one to be rejected.
      // Theoretically, the throttle time should be below or equal to:
      // -(-10) / 2 = 5s
      val (throttleTimeMs1, errors1) = createTopics(TopicsWith30Partitions, StrictCreateTopicsRequestVersion)
      assertThrottleTime(5000, throttleTimeMs1)
      // Ordering is not guaranteed so we only check the errors
      assertEquals(Set(Errors.NONE, Errors.THROTTLING_QUOTA_EXCEEDED), errors1.values.toSet)

      // Retry the rejected topic. It should succeed after the throttling delay is passed and the
      // throttle time should be zero.
      val rejectedTopicName = errors1.filter(_._2 == Errors.THROTTLING_QUOTA_EXCEEDED).keys.head
      val rejectedTopicSpec = TopicsWith30Partitions.filter(_._1 == rejectedTopicName)
      TestUtils.waitUntilTrue(() => {
        val (throttleTimeMs2, errors2) = createTopics(rejectedTopicSpec, StrictCreateTopicsRequestVersion)
        throttleTimeMs2 == 0 && errors2 == Map(rejectedTopicName -> Errors.NONE)
      }, "Failed to create topics after having been throttled")
    }
  }

  @Test
  def testPermissiveCreateTopicsRequest(): Unit = {
    asPrincipal(ThrottledPrincipal) {
      // Create two topics worth of 30 partitions each. As we use a permissive quota, we
      // expect both topics to be created.
      // Theoretically, the throttle time should be below or equal to:
      // -(-40) / 2 = 20s
      val (throttleTimeMs, errors) = createTopics(TopicsWith30Partitions, PermissiveCreateTopicsRequestVersion)
      assertThrottleTime(20000, throttleTimeMs)
      assertEquals(Map(Topic1 -> Errors.NONE, Topic2 -> Errors.NONE), errors)
    }
  }

  @Test
  def testUnboundedCreateTopicsRequest(): Unit = {
    asPrincipal(UnboundedPrincipal) {
      // Create two topics worth of 30 partitions each. As we use an user without quota, we
      // expect both topics to be created. The throttle time should be equal to 0.
      val (throttleTimeMs, errors) = createTopics(TopicsWith30Partitions, StrictCreateTopicsRequestVersion)
      assertEquals(0, throttleTimeMs)
      assertEquals(Map(Topic1 -> Errors.NONE, Topic2 -> Errors.NONE), errors)
    }
  }

  @Test
  def testStrictDeleteTopicsRequest(): Unit = {
    asPrincipal(UnboundedPrincipal) {
      createTopics(TopicsWith30Partitions, StrictCreateTopicsRequestVersion)
    }

    asPrincipal(ThrottledPrincipal) {
      // Delete two topics worth of 30 partitions each. As we use a strict quota, we
      // expect the first topic to be deleted and the second to be rejected.
      // Theoretically, the throttle time should be below or equal to:
      // -(-10) / 2 = 5s
      val (throttleTimeMs1, errors1) = deleteTopics(TopicsWith30Partitions, StrictDeleteTopicsRequestVersion)
      assertThrottleTime(5000, throttleTimeMs1)
      // Ordering is not guaranteed so we only check the errors
      assertEquals(Set(Errors.NONE, Errors.THROTTLING_QUOTA_EXCEEDED), errors1.values.toSet)

      // Retry the rejected topic. It should succeed after the throttling delay is passed and the
      // throttle time should be zero.
      val rejectedTopicName = errors1.filter(_._2 == Errors.THROTTLING_QUOTA_EXCEEDED).keys.head
      val rejectedTopicSpec = TopicsWith30Partitions.filter(_._1 == rejectedTopicName)
      TestUtils.waitUntilTrue(() => {
        val (throttleTimeMs2, errors2) = deleteTopics(rejectedTopicSpec, StrictDeleteTopicsRequestVersion)
        throttleTimeMs2 == 0 && errors2 == Map(rejectedTopicName -> Errors.NONE)
      }, "Failed to delete topics after having been throttled")
    }
  }

  @Test
  def testPermissiveDeleteTopicsRequest(): Unit = {
    asPrincipal(UnboundedPrincipal) {
      createTopics(TopicsWith30Partitions, StrictCreateTopicsRequestVersion)
    }

    asPrincipal(ThrottledPrincipal) {
      // Delete two topics worth of 30 partitions each. As we use a permissive quota, we
      // expect both topics to be deleted.
      // Theoretically, the throttle time should be below or equal to:
      // -(-40) / 2 = 20s
      val (throttleTimeMs, errors) = deleteTopics(TopicsWith30Partitions, PermissiveDeleteTopicsRequestVersion)
      assertThrottleTime(20000, throttleTimeMs)
      assertEquals(Map(Topic1 -> Errors.NONE, Topic2 -> Errors.NONE), errors)
    }
  }

  @Test
  def testUnboundedDeleteTopicsRequest(): Unit = {
    asPrincipal(UnboundedPrincipal) {
      createTopics(TopicsWith30Partitions, StrictCreateTopicsRequestVersion)

      // Delete two topics worth of 30 partitions each. As we use an user without quota, we
      // expect both topics to be deleted. The throttle time should be equal to 0.
      val (throttleTimeMs, errors) = deleteTopics(TopicsWith30Partitions, StrictDeleteTopicsRequestVersion)
      assertEquals(0, throttleTimeMs)
      assertEquals(Map(Topic1 -> Errors.NONE, Topic2 -> Errors.NONE), errors)
    }
  }

  @Test
  def testStrictCreatePartitionsRequest(): Unit = {
    asPrincipal(UnboundedPrincipal) {
      createTopics(TopicsWithOnePartition, StrictCreatePartitionsRequestVersion)
    }

    asPrincipal(ThrottledPrincipal) {
      // Add 30 partitions to each topic. As we use a strict quota, we
      // expect the first topic to be extended and the second to be rejected.
      // Theoretically, the throttle time should be below or equal to:
      // -(-10) / 2 = 5s
      val (throttleTimeMs1, errors1) = createPartitions(TopicsWith31Partitions, StrictCreatePartitionsRequestVersion)
      assertThrottleTime(5000, throttleTimeMs1)
      // Ordering is not guaranteed so we only check the errors
      assertEquals(Set(Errors.NONE, Errors.THROTTLING_QUOTA_EXCEEDED), errors1.values.toSet)

      // Retry the rejected topic. It should succeed after the throttling delay is passed and the
      // throttle time should be zero.
      val rejectedTopicName = errors1.filter(_._2 == Errors.THROTTLING_QUOTA_EXCEEDED).keys.head
      val rejectedTopicSpec = TopicsWith30Partitions.filter(_._1 == rejectedTopicName)
      TestUtils.waitUntilTrue(() => {
        val (throttleTimeMs2, errors2) = createPartitions(rejectedTopicSpec, StrictCreatePartitionsRequestVersion)
        throttleTimeMs2 == 0 && errors2 == Map(rejectedTopicName -> Errors.NONE)
      }, "Failed to create partitions after having been throttled")
    }
  }

  @Test
  def testPermissiveCreatePartitionsRequest(): Unit = {
    asPrincipal(UnboundedPrincipal) {
      createTopics(TopicsWithOnePartition, StrictCreatePartitionsRequestVersion)
    }

    asPrincipal(ThrottledPrincipal) {
      // Create two topics worth of 30 partitions each. As we use a permissive quota, we
      // expect both topics to be created.
      // Theoretically, the throttle time should be below or equal to:
      // -(-40) / 2 = 20s
      val (throttleTimeMs, errors) = createPartitions(TopicsWith31Partitions, PermissiveCreatePartitionsRequestVersion)
      assertThrottleTime(20000, throttleTimeMs)
      assertEquals(Map(Topic1 -> Errors.NONE, Topic2 -> Errors.NONE), errors)
    }
  }

  @Test
  def testUnboundedCreatePartitionsRequest(): Unit = {
    asPrincipal(UnboundedPrincipal) {
      createTopics(TopicsWithOnePartition, StrictCreatePartitionsRequestVersion)

      // Create two topics worth of 30 partitions each. As we use an user without quota, we
      // expect both topics to be created. The throttle time should be equal to 0.
      val (throttleTimeMs, errors) = createPartitions(TopicsWith31Partitions, StrictCreatePartitionsRequestVersion)
      assertEquals(0, throttleTimeMs)
      assertEquals(Map(Topic1 -> Errors.NONE, Topic2 -> Errors.NONE), errors)
    }
  }

  private def assertThrottleTime(max: Int, actual: Int): Unit = {
    assertTrue(
      (actual >= 0) && (actual <= max),
      s"Expected a throttle time between 0 and $max but got $actual")
  }

  private def createTopics(topics: Map[String, Int], version: Short): (Int, Map[String, Errors]) = {
    val data = new CreateTopicsRequestData()
    topics.foreach { case (topic, numPartitions) =>
      data.topics.add(new CreatableTopic()
        .setName(topic).setNumPartitions(numPartitions).setReplicationFactor(1))
    }
    val request = new CreateTopicsRequest.Builder(data).build(version)
    val response = connectAndReceive[CreateTopicsResponse](request)
    response.data.throttleTimeMs -> response.data.topics.asScala
      .map(topic => topic.name -> Errors.forCode(topic.errorCode)).toMap
  }

  private def deleteTopics(topics: Map[String, Int], version: Short): (Int, Map[String, Errors]) = {
    val data = new DeleteTopicsRequestData()
      .setTimeoutMs(60000)
      .setTopicNames(topics.keys.toSeq.asJava)
    val request = new DeleteTopicsRequest.Builder(data).build(version)
    val response = connectAndReceive[DeleteTopicsResponse](request)
    response.data.throttleTimeMs -> response.data.responses.asScala
      .map(topic => topic.name -> Errors.forCode(topic.errorCode)).toMap
  }

  private def createPartitions(topics: Map[String, Int], version: Short): (Int, Map[String, Errors]) = {
    val data = new CreatePartitionsRequestData().setTimeoutMs(60000)
    topics.foreach { case (topic, numPartitions) =>
      data.topics.add(new CreatePartitionsTopic()
        .setName(topic).setCount(numPartitions).setAssignments(null))
    }
    val request = new CreatePartitionsRequest.Builder(data).build(version)
    val response = connectAndReceive[CreatePartitionsResponse](request)
    response.data.throttleTimeMs -> response.data.results.asScala
      .map(topic => topic.name -> Errors.forCode(topic.errorCode)).toMap
  }

  private def defineUserQuota(user: String, quota: Option[Double]): Unit = {
    val entity = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> user).asJava)
    val quotas = Map(DynamicConfig.Client.ControllerMutationOverrideProp -> quota)

    try alterClientQuotas(Map(entity -> quotas))(entity).get(10, TimeUnit.SECONDS) catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private def waitUserQuota(user: String, expectedQuota: Double): Unit = {
    val quotaManager = servers.head.quotaManagers.controllerMutation
    var actualQuota = Double.MinValue

    TestUtils.waitUntilTrue(() => {
      actualQuota = quotaManager.quota(user, "").bound()
      expectedQuota == actualQuota
    }, s"Quota of $user is not $expectedQuota but $actualQuota")
  }

  private def quotaMetric(user: String): Option[KafkaMetric] = {
    val metrics = servers.head.metrics
    val metricName = metrics.metricName(
      "tokens",
      QuotaType.ControllerMutation.toString,
      "Tracking remaining tokens in the token bucket per user/client-id",
      Map(DefaultTags.User -> user, DefaultTags.ClientId -> "").asJava)
    Option(servers.head.metrics.metric(metricName))
  }

  private def waitQuotaMetric(user: String, expectedQuota: Double): Unit = {
    TestUtils.retry(JTestUtils.DEFAULT_MAX_WAIT_MS) {
      quotaMetric(user) match {
        case Some(metric) =>
          val config = metric.config()
          assertEquals(expectedQuota, config.quota().bound(), 0.1)
          assertEquals(ControllerQuotaSamples, config.samples())
          assertEquals(ControllerQuotaWindowSizeSeconds * 1000, config.timeWindowMs())

        case None =>
          fail(s"Quota metric of $user is not defined")
      }
    }
  }

  private def alterClientQuotas(request: Map[ClientQuotaEntity, Map[String, Option[Double]]]): Map[ClientQuotaEntity, KafkaFutureImpl[Void]] = {
    val entries = request.map { case (entity, alter) =>
      val ops = alter.map { case (key, value) =>
        new ClientQuotaAlteration.Op(key, value.map(Double.box).orNull)
      }.asJavaCollection
      new ClientQuotaAlteration(entity, ops)
    }

    val response = request.map(e => e._1 -> new KafkaFutureImpl[Void]).asJava
    sendAlterClientQuotasRequest(entries).complete(response)
    val result = response.asScala
    assertEquals(request.size, result.size)
    request.foreach(e => assertTrue(result.get(e._1).isDefined))
    result.toMap
  }

  private def sendAlterClientQuotasRequest(entries: Iterable[ClientQuotaAlteration]): AlterClientQuotasResponse = {
    val request = new AlterClientQuotasRequest.Builder(entries.asJavaCollection, false).build()
    connectAndReceive[AlterClientQuotasResponse](request, destination = controllerSocketServer)
  }
}

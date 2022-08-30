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

package kafka.network


import com.fasterxml.jackson.databind.ObjectMapper
import kafka.network
import kafka.server.EnvelopeUtils
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.{ConfigResource, SaslConfigs, SslConfigs, TopicConfig}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData._
import org.apache.kafka.common.message.{CreateTopicsRequestData, CreateTopicsResponseData, IncrementalAlterConfigsRequestData}
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AlterConfigsRequest._
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.common.utils.{SecurityUtils, Utils}
import org.apache.kafka.test
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.Mockito.mock

import java.io.IOException
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.Collections
import java.util.concurrent.atomic.AtomicReference
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class RequestChannelTest {
  private val requestChannelMetrics: RequestChannel.Metrics = mock(classOf[RequestChannel.Metrics])
  private val principalSerde = new KafkaPrincipalSerde() {
    override def serialize(principal: KafkaPrincipal): Array[Byte] = Utils.utf8(principal.toString)
    override def deserialize(bytes: Array[Byte]): KafkaPrincipal = SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes))
  }

  @Test
  def testAlterRequests(): Unit = {

    val sensitiveValue = "secret"
    def verifyConfig(resource: ConfigResource, entries: Seq[ConfigEntry], expectedValues: Map[String, String]): Unit = {
      val alterConfigs = request(new AlterConfigsRequest.Builder(
          Collections.singletonMap(resource, new Config(entries.asJavaCollection)), true).build())

      val loggableAlterConfigs = alterConfigs.loggableRequest.asInstanceOf[AlterConfigsRequest]
      val loggedConfig = loggableAlterConfigs.configs.get(resource)
      assertEquals(expectedValues, toMap(loggedConfig))
      val alterConfigsDesc = RequestConvertToJson.requestDesc(alterConfigs.header, alterConfigs.requestLog, alterConfigs.isForwarded).toString
      assertFalse(alterConfigsDesc.contains(sensitiveValue), s"Sensitive config logged $alterConfigsDesc")
    }

    val brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "1")
    val keystorePassword = new ConfigEntry(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sensitiveValue)
    verifyConfig(brokerResource, Seq(keystorePassword), Map(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> Password.HIDDEN))

    val keystoreLocation = new ConfigEntry(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/path/to/keystore")
    verifyConfig(brokerResource, Seq(keystoreLocation), Map(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> "/path/to/keystore"))
    verifyConfig(brokerResource, Seq(keystoreLocation, keystorePassword),
      Map(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> "/path/to/keystore", SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> Password.HIDDEN))

    val listenerKeyPassword = new ConfigEntry(s"listener.name.internal.${SslConfigs.SSL_KEY_PASSWORD_CONFIG}", sensitiveValue)
    verifyConfig(brokerResource, Seq(listenerKeyPassword), Map(listenerKeyPassword.name -> Password.HIDDEN))

    val listenerKeystore = new ConfigEntry(s"listener.name.internal.${SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG}", "/path/to/keystore")
    verifyConfig(brokerResource, Seq(listenerKeystore), Map(listenerKeystore.name -> "/path/to/keystore"))

    val plainJaasConfig = new ConfigEntry(s"listener.name.internal.plain.${SaslConfigs.SASL_JAAS_CONFIG}", sensitiveValue)
    verifyConfig(brokerResource, Seq(plainJaasConfig), Map(plainJaasConfig.name -> Password.HIDDEN))

    val plainLoginCallback = new ConfigEntry(s"listener.name.internal.plain.${SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS}", "test.LoginClass")
    verifyConfig(brokerResource, Seq(plainLoginCallback), Map(plainLoginCallback.name -> plainLoginCallback.value))

    val customConfig = new ConfigEntry("custom.config", sensitiveValue)
    verifyConfig(brokerResource, Seq(customConfig), Map(customConfig.name -> Password.HIDDEN))

    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "testTopic")
    val compressionType = new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    verifyConfig(topicResource, Seq(compressionType), Map(TopicConfig.COMPRESSION_TYPE_CONFIG -> "lz4"))
    verifyConfig(topicResource, Seq(customConfig), Map(customConfig.name -> Password.HIDDEN))

    // Verify empty request
    val alterConfigs = request(new AlterConfigsRequest.Builder(
        Collections.emptyMap[ConfigResource, Config], true).build())
    assertEquals(Collections.emptyMap, alterConfigs.loggableRequest.asInstanceOf[AlterConfigsRequest].configs)
  }

  @Test
  def testIncrementalAlterRequests(): Unit = {

    def incrementalAlterConfigs(resource: ConfigResource,
                                entries: Map[String, String], op: OpType): IncrementalAlterConfigsRequest = {
      val data = new IncrementalAlterConfigsRequestData()
      val alterableConfigs = new AlterableConfigCollection()
      entries.foreach { case (name, value) =>
        alterableConfigs.add(new AlterableConfig().setName(name).setValue(value).setConfigOperation(op.id))
      }
      data.resources.add(new AlterConfigsResource()
        .setResourceName(resource.name).setResourceType(resource.`type`.id)
        .setConfigs(alterableConfigs))
      new IncrementalAlterConfigsRequest.Builder(data).build()
    }

    val sensitiveValue = "secret"
    def verifyConfig(resource: ConfigResource,
                     op: OpType,
                     entries: Map[String, String],
                     expectedValues: Map[String, String]): Unit = {
      val alterConfigs = request(incrementalAlterConfigs(resource, entries, op))
      val loggableAlterConfigs = alterConfigs.loggableRequest.asInstanceOf[IncrementalAlterConfigsRequest]
      val loggedConfig = loggableAlterConfigs.data.resources.find(resource.`type`.id, resource.name).configs
      assertEquals(expectedValues, toMap(loggedConfig))
      val alterConfigsDesc = RequestConvertToJson.requestDesc(alterConfigs.header, alterConfigs.requestLog, alterConfigs.isForwarded).toString
      assertFalse(alterConfigsDesc.contains(sensitiveValue), s"Sensitive config logged $alterConfigsDesc")
    }

    val brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "1")
    val keystorePassword = Map(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> sensitiveValue)
    verifyConfig(brokerResource, OpType.SET, keystorePassword, Map(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> Password.HIDDEN))

    val keystoreLocation = Map(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> "/path/to/keystore")
    verifyConfig(brokerResource, OpType.SET, keystoreLocation, keystoreLocation)
    verifyConfig(brokerResource, OpType.SET, keystoreLocation ++ keystorePassword,
      Map(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> "/path/to/keystore", SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> Password.HIDDEN))

    val listenerKeyPassword = Map(s"listener.name.internal.${SslConfigs.SSL_KEY_PASSWORD_CONFIG}" -> sensitiveValue)
    verifyConfig(brokerResource, OpType.SET, listenerKeyPassword,
      Map(s"listener.name.internal.${SslConfigs.SSL_KEY_PASSWORD_CONFIG}" -> Password.HIDDEN))

    val listenerKeystore = Map(s"listener.name.internal.${SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG}" -> "/path/to/keystore")
    verifyConfig(brokerResource, OpType.SET, listenerKeystore, listenerKeystore)

    val plainJaasConfig = Map(s"listener.name.internal.plain.${SaslConfigs.SASL_JAAS_CONFIG}" -> sensitiveValue)
    verifyConfig(brokerResource, OpType.SET, plainJaasConfig,
      Map(s"listener.name.internal.plain.${SaslConfigs.SASL_JAAS_CONFIG}" -> Password.HIDDEN))

    val plainLoginCallback = Map(s"listener.name.internal.plain.${SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS}" -> "test.LoginClass")
    verifyConfig(brokerResource, OpType.SET, plainLoginCallback, plainLoginCallback)

    val sslProtocols = Map(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG -> "TLSv1.1")
    verifyConfig(brokerResource, OpType.APPEND, sslProtocols, Map(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG -> "TLSv1.1"))
    verifyConfig(brokerResource, OpType.SUBTRACT, sslProtocols, Map(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG -> "TLSv1.1"))
    val cipherSuites = Map(SslConfigs.SSL_CIPHER_SUITES_CONFIG -> null)
    verifyConfig(brokerResource, OpType.DELETE, cipherSuites, cipherSuites)

    val customConfig = Map("custom.config" -> sensitiveValue)
    verifyConfig(brokerResource, OpType.SET, customConfig, Map("custom.config" -> Password.HIDDEN))

    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "testTopic")
    val compressionType = Map(TopicConfig.COMPRESSION_TYPE_CONFIG -> "lz4")
    verifyConfig(topicResource, OpType.SET, compressionType, compressionType)
    verifyConfig(topicResource, OpType.SET, customConfig, Map("custom.config" -> Password.HIDDEN))
  }

  @Test
  def testNonAlterRequestsNotTransformed(): Unit = {
    val metadataRequest = request(new MetadataRequest.Builder(List("topic").asJava, true).build())
    assertSame(metadataRequest.body[MetadataRequest], metadataRequest.loggableRequest)
  }

  @Test
  def testJsonRequests(): Unit = {
    val sensitiveValue = "secret"
    val resource = new ConfigResource(ConfigResource.Type.BROKER, "1")
    val keystorePassword = new ConfigEntry(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sensitiveValue)
    val entries = Seq(keystorePassword)

    val alterConfigs = request(new AlterConfigsRequest.Builder(Collections.singletonMap(resource,
      new Config(entries.asJavaCollection)), true).build())

    assertTrue(isValidJson(RequestConvertToJson.request(alterConfigs.loggableRequest).toString))
  }

  @ParameterizedTest
  @EnumSource(value=classOf[Errors], names=Array("NONE", "CLUSTER_AUTHORIZATION_FAILED", "NOT_CONTROLLER"))
  def testBuildEnvelopeResponse(error: Errors): Unit = {
    val topic = "foo"
    val createTopicRequest = buildCreateTopicRequest(topic)
    val unwrapped = buildUnwrappedEnvelopeRequest(createTopicRequest)

    val createTopicResponse = buildCreateTopicResponse(topic, error)
    val envelopeResponse = buildEnvelopeResponse(unwrapped, createTopicResponse)

    error match {
      case Errors.NOT_CONTROLLER =>
        assertEquals(Errors.NOT_CONTROLLER, envelopeResponse.error)
        assertNull(envelopeResponse.responseData)
      case _ =>
        assertEquals(Errors.NONE, envelopeResponse.error)
        val unwrappedResponse = AbstractResponse.parseResponse(envelopeResponse.responseData, unwrapped.header)
        assertEquals(createTopicResponse.data, unwrappedResponse.data)
    }
  }

  private def buildCreateTopicRequest(topic: String): CreateTopicsRequest = {
    val requestData = new CreateTopicsRequestData()
    requestData.topics.add(new CreatableTopic()
      .setName(topic)
      .setReplicationFactor(-1)
      .setNumPartitions(-1)
    )
    new CreateTopicsRequest.Builder(requestData).build()
  }

  private def buildCreateTopicResponse(
    topic: String,
    error: Errors,
  ): CreateTopicsResponse = {
    val responseData = new CreateTopicsResponseData()
    responseData.topics.add(new CreateTopicsResponseData.CreatableTopicResult()
      .setName(topic)
      .setErrorCode(error.code)
    )
    new CreateTopicsResponse(responseData)
  }

  private def buildUnwrappedEnvelopeRequest(request: AbstractRequest): RequestChannel.Request = {
    val wrappedRequest = TestUtils.buildEnvelopeRequest(
      request,
      principalSerde,
      requestChannelMetrics,
      System.nanoTime()
    )

    val unwrappedRequest = new AtomicReference[RequestChannel.Request]()

    EnvelopeUtils.handleEnvelopeRequest(
      wrappedRequest,
      requestChannelMetrics,
      request => unwrappedRequest.set(request)
    )

    unwrappedRequest.get()
  }

  private def isValidJson(str: String): Boolean = {
    try {
      val mapper = new ObjectMapper
      mapper.readTree(str)
      true
    } catch {
      case _: IOException => false
    }
  }

  def request(req: AbstractRequest): RequestChannel.Request = {
    val buffer = req.serializeWithHeader(new RequestHeader(req.apiKey, req.version, "client-id", 1))
    val requestContext = newRequestContext(buffer)
    new network.RequestChannel.Request(processor = 1,
      requestContext,
      startTimeNanos = 0,
      mock(classOf[MemoryPool]),
      buffer,
      mock(classOf[RequestChannel.Metrics])
    )
  }

  private def newRequestContext(buffer: ByteBuffer): RequestContext = {
    new RequestContext(
      RequestHeader.parse(buffer),
      "connection-id",
      InetAddress.getLoopbackAddress,
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user"),
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
      SecurityProtocol.PLAINTEXT,
      new ClientInformation("name", "version"),
      false)
  }

  private def toMap(config: Config): Map[String, String] = {
    config.entries.asScala.map(e => e.name -> e.value).toMap
  }

  private def toMap(config: IncrementalAlterConfigsRequestData.AlterableConfigCollection): Map[String, String] = {
    config.asScala.map(e => e.name -> e.value).toMap
  }

  private def buildEnvelopeResponse(
    unwrapped: RequestChannel.Request,
    response: AbstractResponse
  ): EnvelopeResponse = {
    assertTrue(unwrapped.envelope.isDefined)
    val envelope = unwrapped.envelope.get

    val send = unwrapped.buildResponseSend(response)
    val sendBytes = test.TestUtils.toBuffer(send)

    // We need to read the size field before `parseResponse` below
    val size = sendBytes.getInt
    assertEquals(size, sendBytes.remaining())
    val envelopeResponse = AbstractResponse.parseResponse(sendBytes, envelope.header)

    assertTrue(envelopeResponse.isInstanceOf[EnvelopeResponse])
    envelopeResponse.asInstanceOf[EnvelopeResponse]
  }
}

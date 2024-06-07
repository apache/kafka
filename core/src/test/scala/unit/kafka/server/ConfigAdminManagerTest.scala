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

import java.util
import java.util.Collections

import kafka.server.metadata.MockConfigRepository
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, BROKER_LOGGER, TOPIC, UNKNOWN}
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.message.{AlterConfigsRequestData, AlterConfigsResponseData, IncrementalAlterConfigsRequestData, IncrementalAlterConfigsResponseData}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterConfigsResource => LAlterConfigsResource}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterConfigsResourceCollection => LAlterConfigsResourceCollection}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterableConfigCollection => LAlterableConfigCollection}
import org.apache.kafka.common.message.AlterConfigsResponseData.{AlterConfigsResourceResponse => LAlterConfigsResourceResponse}
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterableConfig => LAlterableConfig}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResource => IAlterConfigsResource}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResourceCollection => IAlterConfigsResourceCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterableConfig => IAlterableConfig}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterableConfigCollection => IAlterableConfigCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.{AlterConfigsResourceResponse => IAlterConfigsResourceResponse}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors.{INVALID_REQUEST, NONE}
import org.apache.kafka.common.requests.ApiError
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{Assertions, Test}
import org.slf4j.LoggerFactory

class ConfigAdminManagerTest {
  val logger = LoggerFactory.getLogger(classOf[ConfigAdminManagerTest])

  def newConfigAdminManager(brokerId: Integer): ConfigAdminManager = {
    val config = TestUtils.createBrokerConfig(nodeId = brokerId, zkConnect = null)
    new ConfigAdminManager(brokerId, new KafkaConfig(config), new MockConfigRepository())
  }

  def broker0Incremental(): IAlterConfigsResource = new IAlterConfigsResource().
    setResourceName("0").
    setResourceType(BROKER.id()).
    setConfigs(new IAlterableConfigCollection(
      util.Arrays.asList(new IAlterableConfig().setName("foo").
        setValue("bar").
        setConfigOperation(OpType.SET.id())).iterator()))

  def topicAIncremental(): IAlterConfigsResource = new IAlterConfigsResource().
    setResourceName("a").
    setResourceType(TOPIC.id()).
    setConfigs(new IAlterableConfigCollection(
      util.Arrays.asList(new IAlterableConfig().setName("foo").
        setValue("bar").
        setConfigOperation(OpType.SET.id())).iterator()))

  def broker0Legacy(): LAlterConfigsResource = new LAlterConfigsResource().
    setResourceName("0").
    setResourceType(BROKER.id()).
    setConfigs(new LAlterableConfigCollection(
      util.Arrays.asList(new LAlterableConfig().setName("foo").
        setValue("bar")).iterator()))

  def topicALegacy(): LAlterConfigsResource = new LAlterConfigsResource().
    setResourceName("a").
    setResourceType(TOPIC.id()).
    setConfigs(new LAlterableConfigCollection(
      util.Arrays.asList(new LAlterableConfig().setName("foo").
        setValue("bar")).iterator()))

  val invalidRequestError = new ApiError(INVALID_REQUEST)

  @Test
  def testCopyWithoutPreprocessedForIncremental(): Unit = {
    val broker0 = broker0Incremental()
    val topicA = topicAIncremental()
    val request = new IncrementalAlterConfigsRequestData().setValidateOnly(true).
      setResources(new IAlterConfigsResourceCollection(util.Arrays.asList(
        broker0, topicA).iterator()))
    val processed1 = new util.IdentityHashMap[IAlterConfigsResource, ApiError]()
    processed1.put(broker0, ApiError.NONE)
    assertEquals(new IncrementalAlterConfigsRequestData().setValidateOnly(true).
      setResources(new IAlterConfigsResourceCollection(util.Arrays.asList(
        topicA.duplicate()).iterator())),
        ConfigAdminManager.copyWithoutPreprocessed(request, processed1))
    val processed2 = new util.IdentityHashMap[IAlterConfigsResource, ApiError]()
    assertEquals(new IncrementalAlterConfigsRequestData().setValidateOnly(true).
      setResources(new IAlterConfigsResourceCollection(util.Arrays.asList(
        broker0.duplicate(), topicA.duplicate()).iterator())),
      ConfigAdminManager.copyWithoutPreprocessed(request, processed2))
    val processed3 = new util.IdentityHashMap[IAlterConfigsResource, ApiError]()
    processed3.put(broker0, ApiError.NONE)
    processed3.put(topicA, ApiError.NONE)
    assertEquals(new IncrementalAlterConfigsRequestData().setValidateOnly(true),
      ConfigAdminManager.copyWithoutPreprocessed(request, processed3))
  }

  @Test
  def testCopyWithoutPreprocessedForLegacy(): Unit = {
    val broker0 = broker0Legacy()
    val topicA = topicALegacy()
    val request = new AlterConfigsRequestData().setValidateOnly(true).
      setResources(new LAlterConfigsResourceCollection(util.Arrays.asList(
        broker0, topicA).iterator()))
    val processed1 = new util.IdentityHashMap[LAlterConfigsResource, ApiError]()
    processed1.put(broker0, ApiError.NONE)
    assertEquals(new AlterConfigsRequestData().setValidateOnly(true).
      setResources(new LAlterConfigsResourceCollection(util.Arrays.asList(
        topicA.duplicate()).iterator())),
      ConfigAdminManager.copyWithoutPreprocessed(request, processed1))
    val processed2 = new util.IdentityHashMap[LAlterConfigsResource, ApiError]()
    assertEquals(new AlterConfigsRequestData().setValidateOnly(true).
      setResources(new LAlterConfigsResourceCollection(util.Arrays.asList(
        broker0.duplicate(), topicA.duplicate()).iterator())),
      ConfigAdminManager.copyWithoutPreprocessed(request, processed2))
    val processed3 = new util.IdentityHashMap[LAlterConfigsResource, ApiError]()
    processed3.put(broker0, ApiError.NONE)
    processed3.put(topicA, ApiError.NONE)
    assertEquals(new AlterConfigsRequestData().setValidateOnly(true),
      ConfigAdminManager.copyWithoutPreprocessed(request, processed3))
  }

  @Test
  def testReassembleIncrementalResponse(): Unit = {
    val broker0 = broker0Incremental()
    val topicA = topicAIncremental()
    val original = new IncrementalAlterConfigsRequestData().
      setResources(new IAlterConfigsResourceCollection(util.Arrays.asList(
        broker0, topicA).iterator()))
    val preprocessed1 = new util.IdentityHashMap[IAlterConfigsResource, ApiError]()
    preprocessed1.put(broker0, invalidRequestError)
    val persistentResponses1 = new IncrementalAlterConfigsResponseData().setResponses(
      util.Arrays.asList(new IAlterConfigsResourceResponse().
        setResourceName("a").
        setResourceType(TOPIC.id()).
        setErrorCode(NONE.code()).
        setErrorMessage(null)))
    assertEquals(new IncrementalAlterConfigsResponseData().setResponses(
      util.Arrays.asList(new IAlterConfigsResourceResponse().
        setResourceName("0").
        setResourceType(BROKER.id()).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage(INVALID_REQUEST.message()),
        new IAlterConfigsResourceResponse().
          setResourceName("a").
          setResourceType(TOPIC.id()).
          setErrorCode(NONE.code()).
          setErrorMessage(null))),
      ConfigAdminManager.reassembleIncrementalResponse(original, preprocessed1, persistentResponses1))
    val preprocessed2 = new util.IdentityHashMap[IAlterConfigsResource, ApiError]()
    val persistentResponses2 = new IncrementalAlterConfigsResponseData().setResponses(
      util.Arrays.asList(new IAlterConfigsResourceResponse().
          setResourceName("0").
          setResourceType(BROKER.id()).
          setErrorCode(NONE.code()).
          setErrorMessage(null),
        new IAlterConfigsResourceResponse().
          setResourceName("a").
          setResourceType(TOPIC.id()).
          setErrorCode(NONE.code()).
          setErrorMessage(null),
      ))
    assertEquals(new IncrementalAlterConfigsResponseData().setResponses(
      util.Arrays.asList(new IAlterConfigsResourceResponse().
          setResourceName("0").
          setResourceType(BROKER.id()).
          setErrorCode(NONE.code()).
          setErrorMessage(null),
        new IAlterConfigsResourceResponse().
          setResourceName("a").
          setResourceType(TOPIC.id()).
          setErrorCode(NONE.code()).
          setErrorMessage(null))),
      ConfigAdminManager.reassembleIncrementalResponse(original, preprocessed2, persistentResponses2))
  }

  @Test
  def testReassembleLegacyResponse(): Unit = {
    val broker0 = broker0Legacy()
    val topicA = topicALegacy()
    val original = new AlterConfigsRequestData().
      setResources(new LAlterConfigsResourceCollection(util.Arrays.asList(
        broker0, topicA).iterator()))
    val preprocessed1 = new util.IdentityHashMap[LAlterConfigsResource, ApiError]()
    preprocessed1.put(broker0, invalidRequestError)
    val persistentResponses1 = new AlterConfigsResponseData().setResponses(
      util.Arrays.asList(new LAlterConfigsResourceResponse().
        setResourceName("a").
        setResourceType(TOPIC.id()).
        setErrorCode(NONE.code()).
        setErrorMessage(null)))
    assertEquals(new AlterConfigsResponseData().setResponses(
      util.Arrays.asList(new LAlterConfigsResourceResponse().
        setResourceName("0").
        setResourceType(BROKER.id()).
        setErrorCode(INVALID_REQUEST.code()).
        setErrorMessage(INVALID_REQUEST.message()),
        new LAlterConfigsResourceResponse().
          setResourceName("a").
          setResourceType(TOPIC.id()).
          setErrorCode(NONE.code()).
          setErrorMessage(null))),
      ConfigAdminManager.reassembleLegacyResponse(original, preprocessed1, persistentResponses1))
    val preprocessed2 = new util.IdentityHashMap[LAlterConfigsResource, ApiError]()
    val persistentResponses2 = new AlterConfigsResponseData().setResponses(
      util.Arrays.asList(new LAlterConfigsResourceResponse().
        setResourceName("0").
        setResourceType(BROKER.id()).
        setErrorCode(NONE.code()).
        setErrorMessage(null),
        new LAlterConfigsResourceResponse().
          setResourceName("a").
          setResourceType(TOPIC.id()).
          setErrorCode(NONE.code()).
          setErrorMessage(null),
      ))
    assertEquals(new AlterConfigsResponseData().setResponses(
      util.Arrays.asList(new LAlterConfigsResourceResponse().
        setResourceName("0").
        setResourceType(BROKER.id()).
        setErrorCode(NONE.code()).
        setErrorMessage(null),
        new LAlterConfigsResourceResponse().
          setResourceName("a").
          setResourceType(TOPIC.id()).
          setErrorCode(NONE.code()).
          setErrorMessage(null))),
      ConfigAdminManager.reassembleLegacyResponse(original, preprocessed2, persistentResponses2))
  }

  @Test
  def testValidateResourceNameIsCurrentNodeId(): Unit = {
    val manager = newConfigAdminManager(5)
    manager.validateResourceNameIsCurrentNodeId("5")
    assertEquals("Node id must be an integer, but it is: ",
      Assertions.assertThrows(classOf[InvalidRequestException],
        () => manager.validateResourceNameIsCurrentNodeId("")).getMessage)
    assertEquals("Unexpected broker id, expected 5, but received 3",
      Assertions.assertThrows(classOf[InvalidRequestException],
        () => manager.validateResourceNameIsCurrentNodeId("3")).getMessage)
    assertEquals("Node id must be an integer, but it is: e",
      Assertions.assertThrows(classOf[InvalidRequestException],
        () => manager.validateResourceNameIsCurrentNodeId("e")).getMessage)
  }

  def brokerLogger1Incremental(): IAlterConfigsResource = new IAlterConfigsResource().
    setResourceName("1").
    setResourceType(BROKER_LOGGER.id).
    setConfigs(new IAlterableConfigCollection(
      util.Arrays.asList(new IAlterableConfig().setName(logger.getName).
        setValue("INFO").
        setConfigOperation(OpType.SET.id())).iterator()))

  def brokerLogger2Incremental(): IAlterConfigsResource = new IAlterConfigsResource().
    setResourceName("2").
    setResourceType(BROKER_LOGGER.id).
    setConfigs(new IAlterableConfigCollection(
      util.Arrays.asList(new IAlterableConfig().setName(logger.getName).
        setValue(null).
        setConfigOperation(OpType.SET.id())).iterator()))

  @Test
  def testPreprocessIncrementalWithUnauthorizedBrokerLoggerChanges(): Unit = {
    val manager = newConfigAdminManager(1)
    val brokerLogger1 = brokerLogger1Incremental()
    assertEquals(Collections.singletonMap(brokerLogger1,
        new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, null)),
      manager.preprocess(new IncrementalAlterConfigsRequestData().
        setResources(new IAlterConfigsResourceCollection(util.Arrays.asList(
          brokerLogger1).iterator())),
          (_, _) => false))
  }

  @Test
  def testPreprocessIncrementalWithNulls(): Unit = {
    val manager = newConfigAdminManager(2)
    val brokerLogger2 = brokerLogger2Incremental()
    assertEquals(Collections.singletonMap(brokerLogger2,
      new ApiError(INVALID_REQUEST, s"Null value not supported for : ${logger.getName}")),
      manager.preprocess(new IncrementalAlterConfigsRequestData().
        setResources(new IAlterConfigsResourceCollection(util.Arrays.asList(
          brokerLogger2).iterator())),
        (_, _) => true))
  }

  @Test
  def testPreprocessIncrementalWithLoggerChanges(): Unit = {
    val manager = newConfigAdminManager(1)
    val brokerLogger1 = brokerLogger1Incremental()
    assertEquals(Collections.singletonMap(brokerLogger1,
      new ApiError(Errors.NONE, null)),
      manager.preprocess(new IncrementalAlterConfigsRequestData().
        setResources(new IAlterConfigsResourceCollection(util.Arrays.asList(
          brokerLogger1).iterator())),
        (_, _) => true))
  }

  @Test
  def testPreprocessIncrementalWithDuplicates(): Unit = {
    val manager = newConfigAdminManager(1)
    val brokerLogger1a = brokerLogger1Incremental()
    val brokerLogger1b = brokerLogger1Incremental()
    val output = manager.preprocess(new IncrementalAlterConfigsRequestData().
        setResources(new IAlterConfigsResourceCollection(util.Arrays.asList(
          brokerLogger1a, brokerLogger1b).iterator())),
        (_, _) => true)
    assertEquals(2, output.size())
    Seq(brokerLogger1a, brokerLogger1b).foreach(r =>
      assertEquals(new ApiError(INVALID_REQUEST, "Each resource must appear at most once."),
        output.get(r)))
  }

  def brokerLogger1Legacy(): LAlterConfigsResource = new LAlterConfigsResource().
    setResourceName("1").
    setResourceType(BROKER_LOGGER.id).
    setConfigs(new LAlterableConfigCollection(
      util.Arrays.asList(new LAlterableConfig().setName(logger.getName).
        setValue("INFO")).iterator()))

  def broker2Legacy(): LAlterConfigsResource = new LAlterConfigsResource().
    setResourceName("2").
    setResourceType(BROKER.id).
    setConfigs(new LAlterableConfigCollection(
      util.Arrays.asList(new LAlterableConfig().setName(logger.getName).
        setValue(null)).iterator()))

  @Test
  def testPreprocessLegacyWithBrokerLoggerChanges(): Unit = {
    val manager = newConfigAdminManager(1)
    val brokerLogger1 = brokerLogger1Legacy()
    assertEquals(Collections.singletonMap(brokerLogger1,
      new ApiError(INVALID_REQUEST, "Unknown resource type 8")),
      manager.preprocess(new AlterConfigsRequestData().
        setResources(new LAlterConfigsResourceCollection(util.Arrays.asList(
          brokerLogger1).iterator()))))
  }

  @Test
  def testPreprocessLegacyWithNulls(): Unit = {
    val manager = newConfigAdminManager(2)
    val brokerLogger2 = broker2Legacy()
    assertEquals(Collections.singletonMap(brokerLogger2,
      new ApiError(INVALID_REQUEST, s"Null value not supported for : ${logger.getName}")),
      manager.preprocess(new AlterConfigsRequestData().
        setResources(new LAlterConfigsResourceCollection(util.Arrays.asList(
          brokerLogger2).iterator()))))
  }

  @Test
  def testPreprocessLegacyWithDuplicates(): Unit = {
    val manager = newConfigAdminManager(1)
    val brokerLogger1a = brokerLogger1Legacy()
    val brokerLogger1b = brokerLogger1Legacy()
    val output = manager.preprocess(new AlterConfigsRequestData().
      setResources(new LAlterConfigsResourceCollection(util.Arrays.asList(
        brokerLogger1a, brokerLogger1b).iterator())))
    assertEquals(2, output.size())
    Seq(brokerLogger1a, brokerLogger1b).foreach(r =>
      assertEquals(new ApiError(INVALID_REQUEST, "Each resource must appear at most once."),
        output.get(r)))
  }

  def unknownIncremental(): IAlterConfigsResource = new IAlterConfigsResource().
    setResourceName("unknown").
    setResourceType(UNKNOWN.id).
    setConfigs(new IAlterableConfigCollection(
      util.Arrays.asList(new IAlterableConfig().setName("foo").
        setValue("bar").
        setConfigOperation(OpType.SET.id())).iterator()))

  def unknownLegacy(): LAlterConfigsResource = new LAlterConfigsResource().
    setResourceName("unknown").
    setResourceType(UNKNOWN.id).
    setConfigs(new LAlterableConfigCollection(
      util.Arrays.asList(new LAlterableConfig().setName("foo").
        setValue("bar")).iterator()))

  @Test
  def testPreprocessIncrementalWithUnknownResource(): Unit = {
    val manager = newConfigAdminManager(1)
    val unknown = unknownIncremental()
    assertEquals(Collections.singletonMap(unknown,
      new ApiError(INVALID_REQUEST, "Unknown resource type 0")),
        manager.preprocess(new IncrementalAlterConfigsRequestData().
        setResources(new IAlterConfigsResourceCollection(util.Arrays.asList(
          unknown).iterator())),
        (_, _) => false))
  }

  @Test
  def testPreprocessLegacyWithUnknownResource(): Unit = {
    val manager = newConfigAdminManager(1)
    val unknown = unknownLegacy()
    assertEquals(Collections.singletonMap(unknown,
      new ApiError(INVALID_REQUEST, "Unknown resource type 0")),
      manager.preprocess(new AlterConfigsRequestData().
        setResources(new LAlterConfigsResourceCollection(util.Arrays.asList(
          unknown).iterator()))))
  }

  @Test
  def testContainsDuplicates(): Unit = {
    assertFalse(ConfigAdminManager.containsDuplicates(Seq()))
    assertFalse(ConfigAdminManager.containsDuplicates(Seq("foo")))
    assertTrue(ConfigAdminManager.containsDuplicates(Seq("foo", "foo")))
    assertFalse(ConfigAdminManager.containsDuplicates(Seq("foo", "bar", "baz")))
    assertTrue(ConfigAdminManager.containsDuplicates(Seq("foo", "bar", "baz", "foo")))
  }
}
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

import kafka.server.metadata.KRaftMetadataCache
import kafka.server.{DefaultApiVersionManager, ForwardingManager, SimpleApiVersionManager}
import org.apache.kafka.common.errors.{InvalidRequestException, UnsupportedVersionException}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.RequestHeaderData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{RequestHeader, RequestTestUtils}
import org.apache.kafka.server.BrokerFeatures
import org.apache.kafka.server.common.{FinalizedFeatures, KRaftVersion, MetadataVersion}
import org.junit.jupiter.api.Assertions.{assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.Mockito.mock

import java.util.Collections

class ProcessorTest {

  @Test
  def testParseRequestHeaderWithDisabledApiVersion(): Unit = {
    val requestHeader = RequestTestUtils.serializeRequestHeader(
      new RequestHeader(ApiKeys.INIT_PRODUCER_ID, 0, "clientid", 0))
    val apiVersionManager = new SimpleApiVersionManager(ListenerType.CONTROLLER, true,
      () => new FinalizedFeatures(MetadataVersion.latestTesting(), Collections.emptyMap[String, java.lang.Short], 0))
    val e = assertThrows(classOf[InvalidRequestException],
      (() => Processor.parseRequestHeader(apiVersionManager, requestHeader)): Executable,
      "INIT_PRODUCER_ID with listener type CONTROLLER should throw InvalidRequestException exception")
    assertTrue(e.toString.contains("disabled api"));
  }

  @Test
  def testParseRequestHeaderWithUnsupportedApi(): Unit = {
    // We have to use `RequestHeaderData` since `ApiMessageType` doesn't support this protocol api
    val headerVersion = 0.toShort
    val requestHeaderData = new RequestHeaderData()
      .setRequestApiKey(ApiKeys.LEADER_AND_ISR.id)
      .setRequestApiVersion(headerVersion)
      .setClientId("clientid")
      .setCorrelationId(0);
    val requestHeader = RequestTestUtils.serializeRequestHeader(new RequestHeader(requestHeaderData, headerVersion))
    val apiVersionManager = new DefaultApiVersionManager(ListenerType.BROKER, mock(classOf[ForwardingManager]),
      BrokerFeatures.createDefault(true), new KRaftMetadataCache(0, () => KRaftVersion.LATEST_PRODUCTION), true)
    val e = assertThrows(classOf[InvalidRequestException],
      (() => Processor.parseRequestHeader(apiVersionManager, requestHeader)): Executable,
      "LEADER_AND_ISR should throw InvalidRequestException exception")
    assertTrue(e.toString.contains("Unsupported api"));
  }

  @Test
  def testParseRequestHeaderWithUnsupportedApiVersion(): Unit = {
    val requestHeader = RequestTestUtils.serializeRequestHeader(
      new RequestHeader(ApiKeys.FETCH, 0, "clientid", 0))
    val apiVersionManager = new DefaultApiVersionManager(ListenerType.BROKER, mock(classOf[ForwardingManager]),
      BrokerFeatures.createDefault(true), new KRaftMetadataCache(0, () => KRaftVersion.LATEST_PRODUCTION), true)
    val e = assertThrows(classOf[UnsupportedVersionException],
      (() => Processor.parseRequestHeader(apiVersionManager, requestHeader)): Executable,
      "FETCH v0 should throw UnsupportedVersionException exception")
    assertTrue(e.toString.contains("unsupported version"));
  }

  /**
   * We do something unusual with these versions of produce, and we want to make sure we don't regress.
   * See `ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION` for details.
   */
  @Test
  def testParseRequestHeaderForProduceV0ToV2(): Unit = {
    for (version <- 0 to 2) {
      val requestHeader = RequestTestUtils.serializeRequestHeader(
        new RequestHeader(ApiKeys.PRODUCE, version.toShort, "clientid", 0))
      val apiVersionManager = new DefaultApiVersionManager(ListenerType.BROKER, mock(classOf[ForwardingManager]),
        BrokerFeatures.createDefault(true), new KRaftMetadataCache(0, () => KRaftVersion.LATEST_PRODUCTION), true)
      val e = assertThrows(classOf[UnsupportedVersionException],
        (() => Processor.parseRequestHeader(apiVersionManager, requestHeader)): Executable,
        s"PRODUCE $version should throw UnsupportedVersionException exception")
      assertTrue(e.toString.contains("unsupported version"));
    }
  }

}

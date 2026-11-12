/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.network;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.metadata.KRaftMetadataCache;
import org.apache.kafka.server.BrokerFeatures;
import org.apache.kafka.server.DefaultApiVersionManager;
import org.apache.kafka.server.SimpleApiVersionManager;
import org.apache.kafka.server.common.FinalizedFeatures;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class ProcessorTest {

    @Test
    public void testParseRequestHeaderWithDisabledApiVersion() {
        ByteBuffer requestHeader = RequestTestUtils.serializeRequestHeader(
                new RequestHeader(ApiKeys.INIT_PRODUCER_ID, (short) 0, "clientid", 0));
        SimpleApiVersionManager apiVersionManager = new SimpleApiVersionManager(ApiMessageType.ListenerType.CONTROLLER, true,
                () -> FinalizedFeatures.of(MetadataVersion.latestTesting(), Map.of(), 0));
        Throwable e = assertThrows(InvalidRequestException.class,
                () -> Processor.parseRequestHeader(apiVersionManager, requestHeader),
                "INIT_PRODUCER_ID with listener type CONTROLLER should throw InvalidRequestException exception");
        assertTrue(e.toString().contains("disabled api"));
    }

    @Test
    public void testParseRequestHeaderWithUnsupportedApi() {
        // We have to use `RequestHeaderData` since `ApiMessageType` doesn't support this protocol api
        short headerVersion = 0;
        RequestHeaderData requestHeaderData = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.LEADER_AND_ISR.id)
                .setRequestApiVersion(headerVersion)
                .setClientId("clientid")
                .setCorrelationId(0);
        ByteBuffer requestHeader = RequestTestUtils.serializeRequestHeader(new RequestHeader(requestHeaderData, headerVersion));
        @SuppressWarnings("unchecked")
        DefaultApiVersionManager apiVersionManager = new DefaultApiVersionManager(ApiMessageType.ListenerType.BROKER, mock(Supplier.class),
                BrokerFeatures.createDefault(true), new KRaftMetadataCache(0, () -> KRaftVersion.LATEST_PRODUCTION), true, Optional.empty());
        Throwable e = assertThrows(InvalidRequestException.class,
                () -> Processor.parseRequestHeader(apiVersionManager, requestHeader),
                "LEADER_AND_ISR should throw InvalidRequestException exception");
        assertTrue(e.toString().contains("Unsupported api"));
    }

    @Test
    public void testParseRequestHeaderWithUnsupportedApiVersion() {
        ByteBuffer requestHeader = RequestTestUtils.serializeRequestHeader(
                new RequestHeader(ApiKeys.FETCH, (short) 0, "clientid", 0));
        @SuppressWarnings("unchecked")
        DefaultApiVersionManager apiVersionManager = new DefaultApiVersionManager(ApiMessageType.ListenerType.BROKER, mock(Supplier.class),
                BrokerFeatures.createDefault(true), new KRaftMetadataCache(0, () -> KRaftVersion.LATEST_PRODUCTION), true, Optional.empty());
        Throwable e = assertThrows(UnsupportedVersionException.class,
                () -> Processor.parseRequestHeader(apiVersionManager, requestHeader),
                "FETCH v0 should throw UnsupportedVersionException exception");
        assertTrue(e.toString().contains("unsupported version"));
    }

    /**
     * We do something unusual with these versions of produce, and we want to make sure we don't regress.
     * See {@link ApiKeys#PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION} for details.
     */
    @Test
    public void testParseRequestHeaderForProduceV0ToV2() {
        for (short version = 0; version <= 2; version++) {
            ByteBuffer requestHeader = RequestTestUtils.serializeRequestHeader(
                    new RequestHeader(ApiKeys.PRODUCE, version, "clientid", 0));
            @SuppressWarnings("unchecked")
            DefaultApiVersionManager apiVersionManager = new DefaultApiVersionManager(ApiMessageType.ListenerType.BROKER, mock(Supplier.class),
                    BrokerFeatures.createDefault(true), new KRaftMetadataCache(0, () -> KRaftVersion.LATEST_PRODUCTION), true, Optional.empty());
            Throwable e = assertThrows(UnsupportedVersionException.class,
                    () -> Processor.parseRequestHeader(apiVersionManager, requestHeader),
                    "PRODUCE " + version + " should throw UnsupportedVersionException exception");
            assertTrue(e.toString().contains("unsupported version"));
        }
    }

}

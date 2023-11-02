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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.OptionalInt;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AllBrokersStrategyTest {
    private final LogContext logContext = new LogContext();

    @Test
    public void testBuildRequest() {
        AllBrokersStrategy strategy = new AllBrokersStrategy(logContext);
        MetadataRequest.Builder builder = strategy.buildRequest(AllBrokersStrategy.LOOKUP_KEYS);
        assertEquals(Collections.emptyList(), builder.topics());
    }

    @Test
    public void testBuildRequestWithInvalidLookupKeys() {
        AllBrokersStrategy strategy = new AllBrokersStrategy(logContext);
        AllBrokersStrategy.BrokerKey key1 = new AllBrokersStrategy.BrokerKey(OptionalInt.empty());
        AllBrokersStrategy.BrokerKey key2 = new AllBrokersStrategy.BrokerKey(OptionalInt.of(1));
        assertThrows(IllegalArgumentException.class, () -> strategy.buildRequest(mkSet(key1)));
        assertThrows(IllegalArgumentException.class, () -> strategy.buildRequest(mkSet(key2)));
        assertThrows(IllegalArgumentException.class, () -> strategy.buildRequest(mkSet(key1, key2)));

        Set<AllBrokersStrategy.BrokerKey> keys = new HashSet<>(AllBrokersStrategy.LOOKUP_KEYS);
        keys.add(key2);
        assertThrows(IllegalArgumentException.class, () -> strategy.buildRequest(keys));
    }

    @Test
    public void testHandleResponse() {
        AllBrokersStrategy strategy = new AllBrokersStrategy(logContext);

        MetadataResponseData response = new MetadataResponseData();
        response.brokers().add(new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(1)
            .setHost("host1")
            .setPort(9092)
        );
        response.brokers().add(new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(2)
            .setHost("host2")
            .setPort(9092)
        );

        AdminApiLookupStrategy.LookupResult<AllBrokersStrategy.BrokerKey> lookupResult = strategy.handleResponse(
            AllBrokersStrategy.LOOKUP_KEYS,
            new MetadataResponse(response, ApiKeys.METADATA.latestVersion())
        );

        assertEquals(Collections.emptyMap(), lookupResult.failedKeys);

        Set<AllBrokersStrategy.BrokerKey> expectedMappedKeys = mkSet(
            new AllBrokersStrategy.BrokerKey(OptionalInt.of(1)),
            new AllBrokersStrategy.BrokerKey(OptionalInt.of(2))
        );

        assertEquals(expectedMappedKeys, lookupResult.mappedKeys.keySet());
        lookupResult.mappedKeys.forEach((brokerKey, brokerId) -> {
            assertEquals(OptionalInt.of(brokerId), brokerKey.brokerId);
        });
    }

    @Test
    public void testHandleResponseWithNoBrokers() {
        AllBrokersStrategy strategy = new AllBrokersStrategy(logContext);

        MetadataResponseData response = new MetadataResponseData();

        AdminApiLookupStrategy.LookupResult<AllBrokersStrategy.BrokerKey> lookupResult = strategy.handleResponse(
            AllBrokersStrategy.LOOKUP_KEYS,
            new MetadataResponse(response, ApiKeys.METADATA.latestVersion())
        );

        assertEquals(Collections.emptyMap(), lookupResult.failedKeys);
        assertEquals(Collections.emptyMap(), lookupResult.mappedKeys);
    }

    @Test
    public void testHandleResponseWithInvalidLookupKeys() {
        AllBrokersStrategy strategy = new AllBrokersStrategy(logContext);
        AllBrokersStrategy.BrokerKey key1 = new AllBrokersStrategy.BrokerKey(OptionalInt.empty());
        AllBrokersStrategy.BrokerKey key2 = new AllBrokersStrategy.BrokerKey(OptionalInt.of(1));
        MetadataResponse response = new MetadataResponse(new MetadataResponseData(), ApiKeys.METADATA.latestVersion());

        assertThrows(IllegalArgumentException.class, () -> strategy.handleResponse(mkSet(key1), response));
        assertThrows(IllegalArgumentException.class, () -> strategy.handleResponse(mkSet(key2), response));
        assertThrows(IllegalArgumentException.class, () -> strategy.handleResponse(mkSet(key1, key2), response));

        Set<AllBrokersStrategy.BrokerKey> keys = new HashSet<>(AllBrokersStrategy.LOOKUP_KEYS);
        keys.add(key2);
        assertThrows(IllegalArgumentException.class, () -> strategy.handleResponse(keys, response));
    }

}

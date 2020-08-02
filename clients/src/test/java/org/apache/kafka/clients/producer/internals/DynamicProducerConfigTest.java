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
package org.apache.kafka.clients.producer.internals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class DynamicProducerConfigTest {
    private static final long TOPIC_IDLE_MS = 60 * 1000;
    private static final int REQUEST_TIMEOUT = 1000;

    private MockTime time = new MockTime();
    private final LogContext logCtx = new LogContext();
    private ProducerMetadata metadata = new ProducerMetadata(0, Long.MAX_VALUE, TOPIC_IDLE_MS,
            logCtx, new ClusterResourceListeners(), time);
    private MockClient client = new MockClient(time, metadata);
    private DynamicProducerConfig dynamicConfigs;
    Properties props;

    @Before
    public void setup() {
        // This defaults acks to all
        props = TestUtils.producerConfig("", StringSerializer.class, StringSerializer.class);
        // Add a node to metadata for mock request/response
        client.updateMetadata(TestUtils.metadataUpdateWith(1, Collections.singletonMap("test", 1)));
        dynamicConfigs = new DynamicProducerConfig(client, new ProducerConfig(props), time, logCtx, REQUEST_TIMEOUT);
    }


    @Test
    public void testPeriodicFetch() {
        // Send first DescribeConfigsRequest
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));
        assertEquals(1, client.inFlightRequestCount());
    
        // Respond to trigger callback
        client.respond(describeConfigsResponse(Errors.NONE));
        client.poll(REQUEST_TIMEOUT, time.milliseconds());
       
        // Advance clock to before the dynamic config expiration
        time.sleep(15000);
        
        // Not ready to send another request because current configs haven't expired
        assertFalse(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));

        // Advance clock to after expiration
        time.sleep(16000);

        // Now another request should be sent
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));
    }
    
    @Test
    public void testShouldDisableWithInvalidRequestErrorCode() {
        // Send first DescribeConfigsRequest
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));
        assertEquals(1, client.inFlightRequestCount());
    
        // Respond with invalid request error code
        client.respond(describeConfigsResponse(Errors.INVALID_REQUEST));
        client.poll(REQUEST_TIMEOUT, time.milliseconds());
       
        // Make sure that when the INVALID_REQUEST code is recieved in the callback, 
        // the dynamicConfig is disabled
        assertTrue(dynamicConfigs.shouldDisable());
    }

    @Test
    public void testSuccessfulAcksOverride() {
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(Short.valueOf("-1"), dynamicConfigs.getAcks());

        Map<String, String> configs = new HashMap<>();
        configs.put("acks", "0");
        // Send DescribeConfigsResponse with acks value that is a valid override
        client.respond(describeConfigsResponse(configs, Errors.NONE));
        client.poll(REQUEST_TIMEOUT, time.milliseconds());

        // The original acks value should be overriden with the dynamic value
        assertEquals(Short.valueOf("0"), dynamicConfigs.getAcks());
    }

    @Test
    public void testShouldNotOverrideAcksIfUserEnabledIdempotence() {
        // Instiantiate a DynamicProducerConfig with user enabled idempotence
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        DynamicProducerConfig dynamicConfigsWithUserEnabledIdempotence = 
            new DynamicProducerConfig(client, new ProducerConfig(props), time, logCtx, REQUEST_TIMEOUT);


        assertTrue(dynamicConfigsWithUserEnabledIdempotence.maybeFetchConfigs(time.milliseconds()));
        assertEquals(1, client.inFlightRequestCount());

        // Send DescribeConfigsResponse with acks value that is invalid for this client since idempotence was user enabled
        Map<String, String> configs = new HashMap<>();
        configs.put("acks", "0");
        client.respond(describeConfigsResponse(configs, Errors.NONE));
        client.poll(REQUEST_TIMEOUT, time.milliseconds());
        
        // Make sure that we don't override the acks value with a value that is invalid
        assertEquals(Short.valueOf("-1"), dynamicConfigsWithUserEnabledIdempotence.getAcks());
    }

    @Test
    public void testRevertToStaticConfigIfDynamicConfigPairMissing() {
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));
        assertEquals(1, client.inFlightRequestCount());

        // Send DescribeConfigsResponse with acks set
        Map<String, String> configs = new HashMap<>();
        configs.put("acks", "0");
        client.respond(describeConfigsResponse(configs, Errors.NONE));
        client.poll(REQUEST_TIMEOUT, time.milliseconds());
        assertEquals(Short.valueOf("0"), dynamicConfigs.getAcks());
        
        time.sleep(31000);
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));

        // Send DescribeConfigsResponse without setting acks
        configs.remove("acks");
        client.respond(describeConfigsResponse(configs, Errors.NONE));
        client.poll(REQUEST_TIMEOUT, time.milliseconds());
        
        // Reverted to static configuration
        assertEquals(Short.valueOf("-1"), dynamicConfigs.getAcks());
    }
    
    public DescribeConfigsResponse describeConfigsResponse(Errors error) {
        return describeConfigsResponse(Collections.emptyMap(), error);
    }

    public DescribeConfigsResponse describeConfigsResponse(Map<String, String> configs, Errors error) {
        List<DescribeConfigsResponseData.DescribeConfigsResult> results = new ArrayList<DescribeConfigsResponseData.DescribeConfigsResult>();
        DescribeConfigsResponseData.DescribeConfigsResult result = new DescribeConfigsResponseData.DescribeConfigsResult();
        result.setErrorCode(error.code());
        result.setConfigs(createConfigEntries(configs));
        results.add(result);
        return new DescribeConfigsResponse(new DescribeConfigsResponseData().setResults(results));
    }

    public List<DescribeConfigsResponseData.DescribeConfigsResourceResult> createConfigEntries(Map<String, String> configs) {
        List<DescribeConfigsResponseData.DescribeConfigsResourceResult> results = new ArrayList<>();

        configs.entrySet().forEach(entry -> {
            results.add(new DescribeConfigsResponseData.DescribeConfigsResourceResult().setName(entry.getKey()).setValue(entry.getValue()));
        });

        return results;
    }
}

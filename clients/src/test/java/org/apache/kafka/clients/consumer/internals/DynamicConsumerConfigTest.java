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
package org.apache.kafka.clients.consumer.internals;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.requests.DescribeClientConfigsResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class DynamicConsumerConfigTest {

    private static final int METADATA_MAX_AGE = 60 * 5 * 1000;
    private MockTime time = new MockTime();
    private final LogContext logCtx = new LogContext();
    private ConsumerMetadata metadata = new ConsumerMetadata(0, 
            Long.MAX_VALUE, 
            false, 
            false, 
            new SubscriptionState(logCtx, OffsetResetStrategy.NONE), 
            logCtx, 
            new ClusterResourceListeners());
    private MockClient client = new MockClient(time, metadata);
    private ConsumerNetworkClient consumerClient;
    private DynamicConsumerConfig dynamicConfigs;
    private GroupRebalanceConfig rebalanceConfig;
    Properties props = new Properties();

    @Before
    public void setup() {
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        client.updateMetadata(TestUtils.metadataUpdateWith(1, Collections.singletonMap("test", 1)));
        
        consumerClient = new ConsumerNetworkClient(logCtx, client, metadata, time,
                100, 1000, Integer.MAX_VALUE);
        rebalanceConfig = new GroupRebalanceConfig(new ConsumerConfig(props), GroupRebalanceConfig.ProtocolType.CONSUMER);
        dynamicConfigs = new DynamicConsumerConfig(consumerClient, rebalanceConfig, time, logCtx);
    }

    private void addListenerToConfigsFuture(RequestFuture<ClientResponse> configsFuture) {
        if (configsFuture != null) {
            configsFuture.addListener(new RequestFutureListener<ClientResponse>() {
                @Override
                public void onSuccess(ClientResponse resp) {
                    System.out.println("success future");
                    dynamicConfigs.handleConfigsResponse((DescribeClientConfigsResponse) resp.responseBody());
                }
                @Override
                public void onFailure(RuntimeException e) {
                    System.out.println("fail future");
                    dynamicConfigs.handleFailedConfigsResponse();
                }
            });
        }
    }


    @Test
    public void testPeriodicFetch() {
        // Send DescribeConfigsRequest
        RequestFuture<ClientResponse> configsResponse = dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(1, consumerClient.pendingRequestCount());

        addListenerToConfigsFuture(configsResponse);
        // Respond to trigger callback
        client.prepareResponse(describeConfigsResponse(new HashMap<>()));


        consumerClient.poll(time.timer(0));
       
        // Advance clock to before the dynamic config expiration
        time.sleep(METADATA_MAX_AGE - 1000);
        
        // Not ready to send another request because current configs haven't expired
        dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(0, consumerClient.pendingRequestCount());

        // Advance clock to after expiration
        time.sleep(2000);

        // Now another request should be sent
        dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(1, consumerClient.pendingRequestCount());
    }

    @Test
    public void testShouldDisableWithInvalidRequestErrorCode() {
        // Send first DescribeConfigsRequest
        dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(1, consumerClient.pendingRequestCount());
    
        // Respond with invalid request error code to trigger callback
        client.prepareResponse(invalidDescribeConfigsResponse());
        consumerClient.poll(time.timer(0));
       
        // Make sure that this is setting itself to be disabled with INVALID_REQUEST code
        dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(0, consumerClient.pendingRequestCount());
    }

    @Test
    public void testInvalidTimeoutAndHeartbeatConfig() {
        // Send DescribeConfigsRequest
        RequestFuture<ClientResponse> configsResponse = dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(1, consumerClient.pendingRequestCount());

        addListenerToConfigsFuture(configsResponse);

        Map<String, String> configs = new HashMap<>();

        // session.timeout.ms defaults to 10k and heartbeat.interval.ms defaults to 3k
        // the configuration below will be rejected since heartbeat.interval.ms >= session.timeout.ms
        configs.put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, "9000");
        configs.put(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG, "9500");
    
        // Respond to trigger callback
        client.prepareResponse(describeConfigsResponse(configs));
        consumerClient.poll(time.timer(0));

        // Check that static default configs persisted instead of the invalid dynamic configs
        assertEquals(10000, rebalanceConfig.getSessionTimout());
        assertEquals(3000, rebalanceConfig.getHeartbeatInterval());
       
        // Advance clock to before the dynamic config expiration
        time.sleep(METADATA_MAX_AGE - 1000);
        
        // Not ready to send another request because current configs haven't expired
        dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(0, consumerClient.pendingRequestCount());

        // Advance clock to after expiration
        time.sleep(2000);

        // Now another request should be sent
        dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(1, consumerClient.pendingRequestCount());
    }

    @Test
    public void testValidTimeoutAndHeartbeatConfig() {
        // Send DescribeConfigsRequest
        RequestFuture<ClientResponse> configsResponse = dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(1, consumerClient.pendingRequestCount());

        addListenerToConfigsFuture(configsResponse);

        Map<String, String> configs = new HashMap<>();

        // session.timeout.ms defaults to 10k and heartbeat.interval.ms defaults to 3k
        // the configuration below will be accepted since heartbeat.interval.ms < session.timeout.ms
        configs.put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, "12000");
        configs.put(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG, "4000");
    
        // Respond to trigger callback
        client.prepareResponse(describeConfigsResponse(configs));
        consumerClient.poll(time.timer(0));

        // Check that static default configs persisted instead of the invalid dynamic configs
        assertEquals(12000, rebalanceConfig.getSessionTimout());
        assertEquals(4000, rebalanceConfig.getHeartbeatInterval());
       
        // Advance clock to before the dynamic config expiration
        time.sleep(METADATA_MAX_AGE - 1000);
        
        // Not ready to send another request because current configs haven't expired
        dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(0, consumerClient.pendingRequestCount());

        // Advance clock to after expiration
        time.sleep(2000);

        // Now another request should be sent
        dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(1, consumerClient.pendingRequestCount());
    }

    @Test
    public void testRevertToStaticConfigIfDynamicConfigPairMissing() {
        // Send DescribeConfigsRequest
        RequestFuture<ClientResponse> configsResponse = dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(1, consumerClient.pendingRequestCount());

        addListenerToConfigsFuture(configsResponse);

        Map<String, String> configs = new HashMap<>();

        // session.timeout.ms defaults to 10k and heartbeat.interval.ms defaults to 3k
        // the configuration below will be rejected since heartbeat.interval.ms >= session.timeout.ms
        configs.put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, "11000");
        configs.put(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG, "4000");
    
        // Respond to trigger callback
        client.prepareResponse(describeConfigsResponse(configs));
        consumerClient.poll(time.timer(0));

        // Check that the dynamic configs were set
        assertEquals(11000, rebalanceConfig.getSessionTimout());
        assertEquals(4000, rebalanceConfig.getHeartbeatInterval());
       
        // Advance clock to before the dynamic config expiration
        time.sleep(METADATA_MAX_AGE - 1000);
        
        // Not ready to send another request because current configs haven't expired
        dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(0, consumerClient.pendingRequestCount());

        // Advance clock to after expiration
        time.sleep(2000);

        // Now another request should be sent
        configsResponse = dynamicConfigs.maybeFetchConfigs(time.milliseconds());
        assertEquals(1, consumerClient.pendingRequestCount());

        addListenerToConfigsFuture(configsResponse);
        

        // Respond to trigger callback with no dynamic configs
        client.prepareResponse(describeConfigsResponse(new HashMap<>()));
        consumerClient.poll(time.timer(0));

        // Check that this was set back to static configs since no dynamic configs were present
        assertEquals(10000, rebalanceConfig.getSessionTimout());
        assertEquals(3000, rebalanceConfig.getHeartbeatInterval());
    }

    public DescribeClientConfigsResponse invalidDescribeConfigsResponse() {
        return new DescribeClientConfigsResponse(0, new InvalidRequestException("InvalidOperation"));
    }

    public DescribeClientConfigsResponse describeConfigsResponse(Map<String, String> configs) {
        Map<String, String> mockEntity = new HashMap<>();
        mockEntity.put("user", "alice");
        Map<ClientQuotaEntity, Map<String, String>> entityConfigs = new HashMap<>();
        entityConfigs.put(new ClientQuotaEntity(mockEntity), configs);
        return new DescribeClientConfigsResponse(entityConfigs, 0);
    }
}

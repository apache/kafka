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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StreamsKafkaClientTest {

    private static final String TOPIC = "topic";
    private final MockClient kafkaClient = new MockClient(new MockTime());
    private final List<MetricsReporter> reporters = Collections.emptyList();
    private final MetadataResponse metadata = new MetadataResponse(Collections.singletonList(new Node(1, "host", 90)), "cluster", 1, Collections.<MetadataResponse.TopicMetadata>emptyList());
    private final Map<String, Object> config = new HashMap<>();
    private final InternalTopicConfig topicConfigWithNoOverrides = new InternalTopicConfig(TOPIC,
                                                                                           Collections.singleton(InternalTopicConfig.CleanupPolicy.delete),
                                                                                           Collections.<String, String>emptyMap());

    private final Map<String, String> overridenTopicConfig = Collections.singletonMap(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100");
    private final InternalTopicConfig topicConfigWithOverrides = new InternalTopicConfig(TOPIC,
                                                                                         Collections.singleton(InternalTopicConfig.CleanupPolicy.compact),
                                                                                         overridenTopicConfig);


    @Before
    public void before() {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "some_app_id");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
    }

    @Test
    public void testConfigFromStreamsConfig() {
        for (final String expectedMechanism : asList("PLAIN", "SCRAM-SHA-512")) {
            config.put(SaslConfigs.SASL_MECHANISM, expectedMechanism);
            final StreamsConfig streamsConfig = new StreamsConfig(config);
            final AbstractConfig config = StreamsKafkaClient.Config.fromStreamsConfig(streamsConfig);
            assertEquals(expectedMechanism, config.values().get(SaslConfigs.SASL_MECHANISM));
            assertEquals(expectedMechanism, config.getString(SaslConfigs.SASL_MECHANISM));
        }
    }

    @Test
    public void shouldAddCleanupPolicyToTopicConfigWhenCreatingTopic() throws Exception {
        final StreamsKafkaClient streamsKafkaClient = createStreamsKafkaClient();
        verifyCorrectTopicConfigs(streamsKafkaClient, topicConfigWithNoOverrides, Collections.singletonMap("cleanup.policy", "delete"));
    }


    @Test
    public void shouldAddDefaultTopicConfigFromStreamConfig() throws Exception {
        config.put(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_MS_CONFIG), "100");
        config.put(StreamsConfig.topicPrefix(TopicConfig.COMPRESSION_TYPE_CONFIG), "gzip");

        final Map<String, String> expectedConfigs = new HashMap<>();
        expectedConfigs.put(TopicConfig.SEGMENT_MS_CONFIG, "100");
        expectedConfigs.put(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        expectedConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        final StreamsKafkaClient streamsKafkaClient = createStreamsKafkaClient();
        verifyCorrectTopicConfigs(streamsKafkaClient, topicConfigWithNoOverrides, expectedConfigs);
    }

    @Test
    public void shouldSetPropertiesDefinedByInternalTopicConfig() throws Exception {
        final Map<String, String> expectedConfigs = new HashMap<>(overridenTopicConfig);
        expectedConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        final StreamsKafkaClient streamsKafkaClient = createStreamsKafkaClient();
        verifyCorrectTopicConfigs(streamsKafkaClient, topicConfigWithOverrides, expectedConfigs);
    }

    @Test
    public void shouldOverrideDefaultTopicConfigsFromStreamsConfig() throws Exception {
        config.put(StreamsConfig.topicPrefix(TopicConfig.DELETE_RETENTION_MS_CONFIG), "99999");
        config.put(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_MS_CONFIG), "988");

        final Map<String, String> expectedConfigs = new HashMap<>(overridenTopicConfig);
        expectedConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        expectedConfigs.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100");
        expectedConfigs.put(TopicConfig.SEGMENT_MS_CONFIG, "988");
        final StreamsKafkaClient streamsKafkaClient = createStreamsKafkaClient();
        verifyCorrectTopicConfigs(streamsKafkaClient, topicConfigWithOverrides, expectedConfigs);
    }

    @Test
    public void shouldNotAllowNullTopicConfigs() throws Exception {
        config.put(StreamsConfig.topicPrefix(TopicConfig.DELETE_RETENTION_MS_CONFIG), null);
        final StreamsKafkaClient streamsKafkaClient = createStreamsKafkaClient();
        verifyCorrectTopicConfigs(streamsKafkaClient, topicConfigWithNoOverrides, Collections.singletonMap("cleanup.policy", "delete"));
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionOnEmptyBrokerCompatibilityResponse() {
        kafkaClient.prepareResponse(null);
        final StreamsKafkaClient streamsKafkaClient = createStreamsKafkaClient();
        streamsKafkaClient.checkBrokerCompatibility(false);
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionWhenBrokerCompatibilityResponseInconsistent() {
        kafkaClient.prepareResponse(new ProduceResponse(Collections.<TopicPartition, ProduceResponse.PartitionResponse>emptyMap()));
        final StreamsKafkaClient streamsKafkaClient = createStreamsKafkaClient();
        streamsKafkaClient.checkBrokerCompatibility(false);
    }

    @Test(expected = StreamsException.class)
    public void shouldRequireBrokerVersion0101OrHigherWhenEosDisabled() {
        kafkaClient.prepareResponse(new ApiVersionsResponse(Errors.NONE, Collections.singletonList(new ApiVersionsResponse.ApiVersion(ApiKeys.PRODUCE))));
        final StreamsKafkaClient streamsKafkaClient = createStreamsKafkaClient();
        streamsKafkaClient.checkBrokerCompatibility(false);
    }

    @Test(expected = StreamsException.class)
    public void shouldRequireBrokerVersions0110OrHigherWhenEosEnabled() {
        kafkaClient.prepareResponse(new ApiVersionsResponse(Errors.NONE, Collections.singletonList(new ApiVersionsResponse.ApiVersion(ApiKeys.CREATE_TOPICS))));
        final StreamsKafkaClient streamsKafkaClient = createStreamsKafkaClient();
        streamsKafkaClient.checkBrokerCompatibility(true);
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionOnEmptyFetchMetadataResponse() {
        kafkaClient.prepareResponse(null);
        final StreamsKafkaClient streamsKafkaClient = createStreamsKafkaClient();
        streamsKafkaClient.fetchMetadata();
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionWhenFetchMetadataResponseInconsistent() {
        kafkaClient.prepareResponse(new ProduceResponse(Collections.<TopicPartition, ProduceResponse.PartitionResponse>emptyMap()));
        final StreamsKafkaClient streamsKafkaClient = createStreamsKafkaClient();
        streamsKafkaClient.fetchMetadata();
    }

    private void verifyCorrectTopicConfigs(final StreamsKafkaClient streamsKafkaClient,
                                           final InternalTopicConfig internalTopicConfig,
                                           final Map<String, String> expectedConfigs) {
        final Map<String, String> requestedTopicConfigs = new HashMap<>();

        kafkaClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(final AbstractRequest body) {
                if (!(body instanceof CreateTopicsRequest)) {
                    return false;
                }
                final CreateTopicsRequest request = (CreateTopicsRequest) body;
                final Map<String, CreateTopicsRequest.TopicDetails> topics =
                        request.topics();
                final CreateTopicsRequest.TopicDetails topicDetails = topics.get(TOPIC);
                requestedTopicConfigs.putAll(topicDetails.configs);
                return true;
            }
        }, new CreateTopicsResponse(Collections.singletonMap(TOPIC, ApiError.NONE)));

        streamsKafkaClient.createTopics(Collections.singletonMap(internalTopicConfig, 1), 1, 1, metadata);

        assertThat(requestedTopicConfigs, equalTo(expectedConfigs));
    }

    private StreamsKafkaClient createStreamsKafkaClient() {
        final StreamsConfig streamsConfig = new StreamsConfig(config);
        return new StreamsKafkaClient(StreamsKafkaClient.Config.fromStreamsConfig(streamsConfig),
                                                                             kafkaClient,
                                                                             reporters);
    }
}

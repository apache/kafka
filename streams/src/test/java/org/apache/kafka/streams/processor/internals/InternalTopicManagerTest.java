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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.test.MockTimestampExtractor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.processor.internals.InternalTopicManager.WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT;

public class InternalTopicManagerTest {

    private final String topic = "test_topic";
    private final String userEndPoint = "localhost:2171";
    private MockStreamKafkaClient streamsKafkaClient;
    private final Time time = new MockTime();
    @Before
    public void init() {
        final StreamsConfig config = new StreamsConfig(configProps());
        streamsKafkaClient = new MockStreamKafkaClient(config);
    }

    @After
    public void shutdown() throws IOException {
        streamsKafkaClient.close();
    }

    @Test
    public void shouldReturnCorrectPartitionCounts() {
        InternalTopicManager internalTopicManager = new InternalTopicManager(streamsKafkaClient, 1,
            WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT, time);
        Assert.assertEquals(Collections.singletonMap(topic, 1), internalTopicManager.getNumPartitions(Collections.singleton(topic)));
    }

    @Test
    public void shouldCreateRequiredTopics() {
        InternalTopicManager internalTopicManager = new InternalTopicManager(streamsKafkaClient, 1,
            WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT, time);
        internalTopicManager.makeReady(Collections.singletonMap(new InternalTopicConfig(topic, Collections.singleton(InternalTopicConfig.CleanupPolicy.compact), null), 1));
    }

    @Test
    public void shouldNotCreateTopicIfExistsWithDifferentPartitions() {
        InternalTopicManager internalTopicManager = new InternalTopicManager(streamsKafkaClient, 1,
            WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT, time);
        boolean exceptionWasThrown = false;
        try {
            internalTopicManager.makeReady(Collections.singletonMap(new InternalTopicConfig(topic, Collections.singleton(InternalTopicConfig.CleanupPolicy.compact), null), 2));
        } catch (StreamsException e) {
            exceptionWasThrown = true;
        }
        Assert.assertTrue(exceptionWasThrown);
    }

    @Test
    public void shouldNotThrowExceptionIfExistsWithDifferentReplication() {

        // create topic the first time with replication 2
        InternalTopicManager internalTopicManager = new InternalTopicManager(streamsKafkaClient, 2,
            WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT, time);
        internalTopicManager.makeReady(Collections.singletonMap(new InternalTopicConfig(topic, Collections.singleton(InternalTopicConfig.CleanupPolicy.compact), null), 1));

        // attempt to create it again with replication 1
        InternalTopicManager internalTopicManager2 = new InternalTopicManager(streamsKafkaClient, 1,
            WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT, time);
        try {
            internalTopicManager2.makeReady(Collections.singletonMap(new InternalTopicConfig(topic, Collections.singleton(InternalTopicConfig.CleanupPolicy.compact), null), 1));
        } catch (StreamsException e) {
            Assert.fail("did not expect an exception since topic is already there.");
        }
    }

    @Test
    public void shouldNotThrowExceptionForEmptyTopicMap() {
        InternalTopicManager internalTopicManager = new InternalTopicManager(streamsKafkaClient, 1,
            WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT, time);
        internalTopicManager.makeReady(Collections.EMPTY_MAP);
    }

    private Properties configProps() {
        return new Properties() {
            {
                setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "Internal-Topic-ManagerTest");
                setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, userEndPoint);
                setProperty(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
                setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName());
            }
        };
    }

    private class MockStreamKafkaClient extends StreamsKafkaClient {

        MockStreamKafkaClient(final StreamsConfig streamsConfig) {
            super(StreamsKafkaClient.Config.fromStreamsConfig(streamsConfig), new MockClient(new MockTime()), Collections.EMPTY_LIST);
        }

        @Override
        public void createTopics(final Map<InternalTopicConfig, Integer> topicsMap, final int replicationFactor,
                                 final long windowChangeLogAdditionalRetention, final MetadataResponse metadata) {
            // do nothing
        }

        @Override
        public MetadataResponse fetchMetadata() {
            Node node = new Node(1, "host1", 1001);
            MetadataResponse.PartitionMetadata partitionMetadata = new MetadataResponse.PartitionMetadata(Errors.NONE, 1, node, new ArrayList<Node>(), new ArrayList<Node>(), new ArrayList<Node>());
            MetadataResponse.TopicMetadata topicMetadata = new MetadataResponse.TopicMetadata(Errors.NONE, topic, true, Collections.singletonList(partitionMetadata));
            MetadataResponse response = new MetadataResponse(Collections.<Node>singletonList(node), null, MetadataResponse.NO_CONTROLLER_ID,
                Collections.singletonList(topicMetadata));
            return response;
        }
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.test.MockTimestampExtractor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Arrays;
import java.util.ArrayList;

import static org.apache.kafka.streams.processor.internals.InternalTopicManager.WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT;

public class InternalTopicManagerTest {

    private String userEndPoint = "localhost:2171";
    StreamsConfig config;
    MockStreamKafkaClient streamsKafkaClient;

    @Before
    public void init() {
        config = new StreamsConfig(configProps());
        streamsKafkaClient = new MockStreamKafkaClient(config);
    }

    @Test
    public void shouldCreateRequiredTopics() throws Exception {

        streamsKafkaClient.setReturnCorrectTopic(true);
        InternalTopicManager internalTopicManager = new InternalTopicManager(streamsKafkaClient, 1, WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT);
        internalTopicManager.makeReady(new InternalTopicConfig("test_topic", Collections.singleton(InternalTopicConfig.CleanupPolicy.compact), null), 1);
    }

    @Test
    public void shouldNotCreateTopicIfExistsWithDifferentPartitions() throws Exception {

        streamsKafkaClient.setReturnCorrectTopic(true);
        InternalTopicManager internalTopicManager = new InternalTopicManager(streamsKafkaClient, 1, WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT);
        boolean exceptionWasThrown = false;
        try {
            internalTopicManager.makeReady(new InternalTopicConfig("test_topic", Collections.singleton(InternalTopicConfig.CleanupPolicy.compact), null), 2);
        } catch (StreamsException e) {
            exceptionWasThrown = true;
        }
        Assert.assertTrue(exceptionWasThrown);
    }

    private Properties configProps() {
        return new Properties() {
            {
                setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "Internal-Topic-ManagerTest");
                setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, userEndPoint);
                setProperty(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3");
                setProperty(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName());
            }
        };
    }

    private class MockStreamKafkaClient extends StreamsKafkaClient {
        public MockStreamKafkaClient(final StreamsConfig streamsConfig) {
            super(streamsConfig);
        }

        public boolean isReturnCorrectTopic() {
            return returnCorrectTopic;
        }

        public void setReturnCorrectTopic(boolean returnCorrectTopic) {
            this.returnCorrectTopic = returnCorrectTopic;
        }

        boolean returnCorrectTopic = false;


        @Override
        public void createTopics(final Map<InternalTopicConfig, Integer> topicsMap, final int replicationFactor, final long windowChangeLogAdditionalRetention) {

        }

        @Override
        public MetadataResponse.TopicMetadata fetchTopicMetadata(final String topic) {

            if (returnCorrectTopic) {
                MetadataResponse.PartitionMetadata partitionMetadata = new MetadataResponse.PartitionMetadata(Errors.NONE, 1, null, new ArrayList<Node>(), new ArrayList<Node>());
                MetadataResponse.TopicMetadata topicMetadata = new MetadataResponse.TopicMetadata(Errors.NONE, topic, true, Arrays.asList(partitionMetadata));
                return topicMetadata;
            }
            return null;
        }

        @Override
        public Collection<MetadataResponse.TopicMetadata> fetchTopicsMetadata() {
            if (returnCorrectTopic) {
                return Arrays.asList(fetchTopicMetadata("test_topic"));
            }
            return null;
        }
    }
}
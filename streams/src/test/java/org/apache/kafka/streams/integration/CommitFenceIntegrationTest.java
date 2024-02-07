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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.test.IntegrationTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

@Category({IntegrationTest.class})
public class CommitFenceIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final String APP_ID = "commit-fence-integration-test";
    static final String INPUT_TOPIC_LEFT = "inputTopicLeft";
    public static final TopicPartition TOPIC_PARTITION = new TopicPartition(INPUT_TOPIC_LEFT, 0);

    @Before
    public void prepareTopology() throws InterruptedException {
        CLUSTER.createTopics(INPUT_TOPIC_LEFT);
    }

    @After
    public void cleanup() throws InterruptedException {
        System.out.println("CLEANUP");
        CLUSTER.deleteAllTopicsAndWait(120000);
    }

    @Test
    public void temp() throws InterruptedException, ExecutionException {
        final KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(mkMap(
            mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
            mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
            mkEntry(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "20000"),
            mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "mygroup")
        ));

        final KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(mkMap(
            mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
            mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
            mkEntry(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "20000"),
            mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "mygroup")
        ));

        final AdminClient adminClient = AdminClient.create(mkMap(mkEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())));

        consumer1.subscribe(Arrays.asList(INPUT_TOPIC_LEFT));
        final ConsumerRecords<String, String> poll = consumer1.poll(Duration.ofSeconds(2));
        System.out.println(poll);
        System.out.println(consumer1.assignment());
        System.out.println(consumer1.assignment().stream().map(consumer1::position).collect(Collectors.toList()));
        commit(consumer1, 10L);
        System.out.println(consumer1.assignment().stream().map(consumer1::position).collect(Collectors.toList()));
        final Map<String, ConsumerGroupDescription> mygroup = adminClient.describeConsumerGroups(Arrays.asList("mygroup")).all().get();
        final Map<TopicPartition, OffsetAndMetadata> mygroup1 = adminClient.listConsumerGroupOffsets("mygroup").partitionsToOffsetAndMetadata().get();
        commit(consumer1, 9L);
        final Map<TopicPartition, OffsetAndMetadata> mygroup2 = adminClient.listConsumerGroupOffsets("mygroup").partitionsToOffsetAndMetadata().get();
        consumer2.subscribe(Arrays.asList(INPUT_TOPIC_LEFT));

        ConsumerGroupState state = null;
        while (state == null || state == ConsumerGroupState.PREPARING_REBALANCE) {
            final ConsumerRecords<String, String> poll5 = consumer2.poll(Duration.ofSeconds(3));
            final ConsumerRecords<String, String> poll6 = consumer1.poll(Duration.ofSeconds(3));
            final Map<String, ConsumerGroupDescription> my2group = adminClient.describeConsumerGroups(Arrays.asList("mygroup")).all().get();
            final ConsumerGroupDescription description = my2group.get("mygroup");
            state = description.state();
        }

        final ConsumerRecords<String, String> poll5 = consumer2.poll(Duration.ofSeconds(3));
        final ConsumerRecords<String, String> poll6 = consumer1.poll(Duration.ofSeconds(3));
        final Map<String, ConsumerGroupDescription> my2group = adminClient.describeConsumerGroups(Arrays.asList("mygroup")).all().get();
        System.out.println(my2group);
        final Map<TopicPartition, OffsetAndMetadata> my2group1 = adminClient.listConsumerGroupOffsets("mygroup").partitionsToOffsetAndMetadata().get();
        System.out.println(my2group1);
        if (consumer1.assignment().isEmpty()) {
            System.out.println("cons2 owns it");
            commit(consumer2, 11L);
            commit(consumer1, 8L);
        } else {
            System.out.println("cons1 owns it");
            commit(consumer1, 11L);
            commit(consumer2, 8L);
        }
        final Map<TopicPartition, OffsetAndMetadata> my2group2 = adminClient.listConsumerGroupOffsets("mygroup").partitionsToOffsetAndMetadata().get();
        System.out.println(my2group2);
        System.out.println("end");
        MatcherAssert.assertThat(my2group2.toString(), my2group2.get(TOPIC_PARTITION).offset(), CoreMatchers.is(11L));
    }

    private static void commit(final KafkaConsumer<String, String> consumer, final long offset) {
        final String metadata = consumer.assignment().toString();
        consumer.commitSync(mkMap(mkEntry(TOPIC_PARTITION, new OffsetAndMetadata(offset, metadata))));
        System.out.println("Committed " + offset + " with assignment " + metadata);
    }
}

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

import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;

@Category({IntegrationTest.class})
public class TaskIdlingIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);
    private final static int NUM_BROKERS = 1;

    public final static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;
    private final static String STREAM_1 = "STREAM_1";
    private final static String STREAM_2 = "STREAM_2";
    private final static String STREAM_3 = "STREAM_3";
    private final static String STREAM_4 = "STREAM_4";
    private final Properties streamsConfig = Utils.mkProperties(
        Utils.mkMap(
            Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "TaskIdlingIT"),
            Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            Utils.mkEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        )
    );
    private static Properties consumerConfig;
    private static Properties producerConfig;

    @BeforeClass
    public static void startCluster() throws IOException, InterruptedException {
        CLUSTER.start();
        //Use multiple partitions to ensure distribution of keys.
        consumerConfig = Utils.mkProperties(
            Utils.mkMap(
                Utils.mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                Utils.mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
                Utils.mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName())
            )
        );

        producerConfig = Utils.mkProperties(
            Utils.mkMap(
                Utils.mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                Utils.mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName()),
                Utils.mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName())
            )
        );

        CLUSTER.createTopic(STREAM_1, 1, 1);
        CLUSTER.createTopic(STREAM_2, 1, 1);
        CLUSTER.createTopic(STREAM_3, 1, 1);
        CLUSTER.createTopic(STREAM_4, 1, 1);

        try (final Producer<Object, Object> producer = new KafkaProducer<>(producerConfig)) {
            final String[] inputs = {STREAM_1, STREAM_2, STREAM_3};
            for (int i = 0; i < 10_000; i++) {
                for (final String input : inputs) {
                    producer.send(
                        new ProducerRecord<>(
                            input,
                            null,
                            ((Time) MOCK_TIME).milliseconds(),
                            i,
                            i,
                            null
                        )
                    );
                    ((Time) MOCK_TIME).sleep(1L);
                }
            }
            producer.flush();
        }
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Before
    public void before() throws IOException {
        final String stateDirBasePath = TestUtils.tempDirectory().getPath();
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, stateDirBasePath + "-1");
    }

    @After
    public void after() throws IOException {
        IntegrationTestUtils.purgeLocalStreamsState(Collections.singletonList(streamsConfig));
    }

    @Test
    public void shouldInnerJoinMultiPartitionQueryable() throws Exception {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<Integer, Integer> stream1 =
            streamsBuilder.stream(STREAM_1, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        final KStream<Integer, Integer> stream2 =
            streamsBuilder.stream(STREAM_2, Consumed.with(Serdes.Integer(), Serdes.Integer()));
        final KStream<Integer, Integer> stream3 =
            streamsBuilder.stream(STREAM_3, Consumed.with(Serdes.Integer(), Serdes.Integer()));

        final KStream<Integer, Integer> merge = stream1.merge(stream2).merge(stream3);
        final ConcurrentLinkedDeque<KeyValue<Integer, Integer>> mapSeen = new ConcurrentLinkedDeque<>();
        final KStream<Integer, Integer> map = merge.map((key, value) -> {
            final KeyValue<Integer, Integer> keyValue = new KeyValue<>(key, value);
            mapSeen.offer(keyValue);
            return keyValue;
        });
        streamsBuilder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("STORE"),
                Serdes.Integer(),
                Serdes.Integer()
            )
        );
        final ConcurrentLinkedDeque<Record<Integer, Integer>> processSeen = new ConcurrentLinkedDeque<>();
        final KStream<Integer, String> process = map.process(
            () -> new ContextualProcessor<Integer, Integer, Integer, String>() {

                private KeyValueStore<Integer, Integer> store;

                @Override
                public void init(final ProcessorContext<Integer, String> context) {
                    super.init(context);
                    store = context.getStateStore("STORE");
                }

                @Override
                public void process(final Record<Integer, Integer> record) {
                    processSeen.offer(record);
                    store.put(record.key(), record.value());
                    final String topic = String.format(
                        "%s %d %d",
                        context().recordMetadata().get().topic(),
                        context().recordMetadata().get().partition(),
                        record.timestamp()
                    );
                    context().forward(record.withValue(topic));
                }
            },
            "STORE"
        );

        process.to(STREAM_4, Produced.with(Serdes.Integer(), Serdes.String()));

        final ArrayList<ConsumerRecord<Integer, String>> consumerSeen = new ArrayList<>(10_000 * 3);

        try (
            final KafkaStreams runningStreams = IntegrationTestUtils.getRunningStreams(streamsConfig, streamsBuilder, true);
            final KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerConfig);
        ) {
            final Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            final List<TopicPartition> partitions =
                topics
                    .get(STREAM_4)
                    .stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .collect(Collectors.toList());

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            while (consumerSeen.size() < (10_000 * 3)) {
                final ConsumerRecords<Integer, String> poll = consumer.poll(Duration.ofMillis(100L));
                for (final ConsumerRecord<Integer, String> record : poll) {
                    System.out.println(record.key() + " " + record.value());
                    consumerSeen.add(record);
                }
            }
        }

        long lastTimestamp = -1;
        int consumeIdx = 0;
        for (int i = 0; i < 10_000; i++) {
            for (int j = 0; j < 3; j++) {
                assertThat(mapSeen.poll().key, Matchers.is(i));
                final Record<Integer, Integer> processRecord = processSeen.poll();
                assertThat(processRecord.key(), Matchers.is(i));
                assertThat(processRecord.timestamp(), Matchers.greaterThan(lastTimestamp));
                lastTimestamp = processRecord.timestamp();
                final ConsumerRecord<Integer, String> consumerRecord = consumerSeen.get(consumeIdx++);
                assertThat(consumerRecord.key(), Matchers.is(i));
            }
        }
    }

}

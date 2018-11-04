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
package org.apache.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TopologyDiskAccessTest {

    private Properties streamsConfiguration;
    private String inputTopic;
    private String outputTopic;
    private String globalSourceTopicName;

    @Before
    public void setUp() {
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG,
                TestUtils.IO_TMP_DIR + File.separator + "kafka-" + TestUtils.randomString(5));
    }

    @Test
    public void statelessTopologiesShouldNotCreateDirectories() {
        updateStreamsAppId();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        input.filter((k, s) -> s.length() % 2 == 0)
                .map((k, v) -> new KeyValue<>(k, k + v))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        startStreamsAndCheckDirExistence(builder.build(), Collections.singleton(inputTopic));
    }

    @Test
    public void statelessPAPITopologiesShouldNotCreateDirectories() {
        updateStreamsAppId();

        final Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), inputTopic)
                .addProcessor("process", () -> new Processor<String, String>() {
                    private ProcessorContext context;
                    @Override
                    public void init(final ProcessorContext context) {
                        this.context = context;
                    }

                    @Override
                    public void process(final String key, final String value) {
                        if (value.length() % 2 == 0) {
                            context.forward(key, key + value);
                        }
                    }

                    @Override
                    public void close() {
                    }
                }, "source")
                .addSink("sink", outputTopic, new StringSerializer(), new StringSerializer(), "process");

        startStreamsAndCheckDirExistence(topology, Collections.singleton(inputTopic));
    }

    @Test
    public void inMemoryStatefulTopologiesShouldNotCreateDirectories() {
        updateStreamsAppId();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        final KTable<String, Long> counts = input.filter((k, s) -> s.length() % 2 == 0)
                .map((k, v) -> new KeyValue<>(k, k + v))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as(Stores.inMemoryKeyValueStore("counts")));

        counts.toStream()
                .map((key, value) -> new KeyValue<>(key, value.toString()))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        builder.globalTable(globalSourceTopicName, Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as(Stores.inMemoryKeyValueStore("globalStore")));
        startStreamsAndCheckDirExistence(builder.build(), Arrays.asList(inputTopic, globalSourceTopicName));
    }

    @Test
    public void inMemoryStatefulPAPITopologiesShouldNotCreateDirectories() {
        updateStreamsAppId();

        final String storeName = "counts";
        final StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(storeName), Serdes.String(), Serdes.Long());
        final Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), inputTopic)
                .addProcessor("process", () -> new WordCountProcessor(storeName), "source")
                .addStateStore(storeBuilder, "process")
                .addSink("sink", outputTopic, new StringSerializer(), new StringSerializer(), "process");

        final StoreBuilder<KeyValueStore<String, String>> globalStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("globalStore"), Serdes.String(), Serdes.String()).withLoggingDisabled();
        topology.addGlobalStore(globalStoreBuilder,
                globalSourceTopicName,
                Serdes.String().deserializer(),
                Serdes.String().deserializer(),
                globalSourceTopicName,
                globalSourceTopicName + "-processor",
                new MockProcessorSupplier());

        startStreamsAndCheckDirExistence(topology, Arrays.asList(inputTopic, globalSourceTopicName));
    }

    private void updateStreamsAppId() {
        final String applicationId = UUID.randomUUID().toString();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        inputTopic = "input" + applicationId;
        outputTopic = "output" + applicationId;
        globalSourceTopicName = "global" + applicationId;
    }

    @SuppressWarnings("unchecked")
    private void startStreamsAndCheckDirExistence(final Topology topology, final Collection<String> topics) {
        final File baseDir = new File(streamsConfiguration.getProperty(StreamsConfig.STATE_DIR_CONFIG));
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfiguration)) {
            final ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(Serdes.String().serializer(),
                    Serdes.String().serializer());
            for (final String topic : topics) {
                driver.pipeInput(factory.create(topic, Arrays.asList(new KeyValue("a", "de"), new KeyValue("c", "de"))));
            }

            driver.readOutput(outputTopic, new StringDeserializer(), new StringDeserializer());

            // streams should not create base directory if it doesn't exists for stateless / in-memory topology.
            if (baseDir.exists()) {
                final Path root = Paths.get(baseDir.getPath());
                try {
                    final List<Path> collect = Files.find(root, 999, (p, bfa) -> true).collect(Collectors.toList());
                    Assert.fail("Files should not have existed, but it did: " + collect);
                } catch (final IOException e) {
                    Assert.fail("Couldn't read the state directory : " + baseDir.getPath());
                }
            }
        } finally {
            try {
                Utils.delete(baseDir);
            } catch (final IOException e) {
                // Ideally, the test case should fail before the call reaches here
                Assert.fail("Unable to delete the base directory : " + baseDir.getPath());
            }
        }
    }

    private static class WordCountProcessor implements Processor<String, String> {

        private ProcessorContext context;
        private KeyValueStore<String, Long> kvStore;
        private final String storeName;
        private final Pattern pattern = Pattern.compile("\\s+");

        private WordCountProcessor(final String storeName) {
            this.storeName = storeName;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(final ProcessorContext context) {
            this.context = context;
            kvStore = (KeyValueStore<String, Long>) context.getStateStore(storeName);
            this.context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
                final KeyValueIterator<String, Long> iter = this.kvStore.all();
                while (iter.hasNext()) {
                    final KeyValue<String, Long> entry = iter.next();
                    context.forward(entry.key, entry.value.toString());
                }
                iter.close();
                context.commit();
            });
        }

        @Override
        public void process(final String key, final String value) {
            final String[] words = pattern.split(value);
            for (final String word : words) {
                final Long oldValue = kvStore.get(word);
                if (oldValue == null) {
                    kvStore.put(word, 1L);
                } else {
                    kvStore.put(word, oldValue + 1);
                }
            }
        }

        @Override
        public void close() {
        }
    }

}

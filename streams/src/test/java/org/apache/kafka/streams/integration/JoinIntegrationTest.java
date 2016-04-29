/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.integration;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * End-to-end integration test that demonstrates how to perform a join between a KStream and a
 * KTable (think: KStream.leftJoin(KTable)), i.e. an example of a stateful computation.
 */
public class JoinIntegrationTest {
    @ClassRule
    public static EmbeddedSingleNodeKafkaCluster cluster = new EmbeddedSingleNodeKafkaCluster();
    private static final String USER_CLICKS_TOPIC = "user-clicks";
    private static final String USER_REGIONS_TOPIC = "user-regions";
    private static final String OUTPUT_TOPIC = "output-topic";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        cluster.createTopic(USER_CLICKS_TOPIC);
        cluster.createTopic(USER_REGIONS_TOPIC);
        cluster.createTopic(OUTPUT_TOPIC);
    }

    /**
     * Tuple for a region and its associated number of clicks.
     */
    private static final class RegionWithClicks {

        private final String region;
        private final long clicks;

        public RegionWithClicks(String region, long clicks) {
            if (region == null || region.isEmpty()) {
                throw new IllegalArgumentException("region must be set");
            }
            if (clicks < 0) {
                throw new IllegalArgumentException("clicks must not be negative");
            }
            this.region = region;
            this.clicks = clicks;
        }

        public String getRegion() {
            return region;
        }

        public long getClicks() {
            return clicks;
        }

    }

    @Test
    public void shouldCountClicksPerRegion() throws Exception {
        // Input 1: Clicks per user (multiple records allowed per user).
        List<KeyValue<String, Long>> userClicks = Arrays.asList(
            new KeyValue<>("alice", 13L),
            new KeyValue<>("bob", 4L),
            new KeyValue<>("chao", 25L),
            new KeyValue<>("bob", 19L),
            new KeyValue<>("dave", 56L),
            new KeyValue<>("eve", 78L),
            new KeyValue<>("alice", 40L),
            new KeyValue<>("fang", 99L)
        );

        // Input 2: Region per user (multiple records allowed per user).
        List<KeyValue<String, String>> userRegions = Arrays.asList(
            new KeyValue<>("alice", "asia"),   /* Alice lived in Asia originally... */
            new KeyValue<>("bob", "americas"),
            new KeyValue<>("chao", "asia"),
            new KeyValue<>("dave", "europe"),
            new KeyValue<>("alice", "europe"), /* ...but moved to Europe some time later. */
            new KeyValue<>("eve", "americas"),
            new KeyValue<>("fang", "asia")
        );

        List<KeyValue<String, Long>> expectedClicksPerRegion = Arrays.asList(
            new KeyValue<>("europe", 13L),
            new KeyValue<>("americas", 4L),
            new KeyValue<>("asia", 25L),
            new KeyValue<>("americas", 23L),
            new KeyValue<>("europe", 69L),
            new KeyValue<>("americas", 101L),
            new KeyValue<>("europe", 109L),
            new KeyValue<>("asia", 124L)
        );

        //
        // Step 1: Configure and start the processor topology.
        //
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, cluster.zKConnectString());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Explicitly place the state directory under /tmp so that we can remove it via
        // `purgeLocalStreamsState` below.  Once Streams is updated to expose the effective
        // StreamsConfig configuration (so we can retrieve whatever state directory Streams came up
        // with automatically) we don't need to set this anymore and can update `purgeLocalStreamsState`
        // accordingly.
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

        KStreamBuilder builder = new KStreamBuilder();

        // This KStream contains information such as "alice" -> 13L.
        //
        // Because this is a KStream ("record stream"), multiple records for the same user will be
        // considered as separate click-count events, each of which will be added to the total count.
        KStream<String, Long> userClicksStream = builder.stream(stringSerde, longSerde, USER_CLICKS_TOPIC);

        // This KTable contains information such as "alice" -> "europe".
        //
        // Because this is a KTable ("changelog stream"), only the latest value (here: region) for a
        // record key will be considered at the time when a new user-click record (see above) is
        // received for the `leftJoin` below.  Any previous region values are being considered out of
        // date.  This behavior is quite different to the KStream for user clicks above.
        //
        // For example, the user "alice" will be considered to live in "europe" (although originally she
        // lived in "asia") because, at the time her first user-click record is being received and
        // subsequently processed in the `leftJoin`, the latest region update for "alice" is "europe"
        // (which overrides her previous region value of "asia").
        KTable<String, String> userRegionsTable =
            builder.table(stringSerde, stringSerde, USER_REGIONS_TOPIC);

        // Compute the number of clicks per region, e.g. "europe" -> 13L.
        //
        // The resulting KTable is continuously being updated as new data records are arriving in the
        // input KStream `userClicksStream` and input KTable `userRegionsTable`.
        KTable<String, Long> clicksPerRegion = userClicksStream
            // Join the stream against the table.
            //
            // Null values possible: In general, null values are possible for region (i.e. the value of
            // the KTable we are joining against) so we must guard against that (here: by setting the
            // fallback region "UNKNOWN").  In this specific example this is not really needed because
            // we know, based on the test setup, that all users have appropriate region entries at the
            // time we perform the join.
            //
            // Also, we need to return a tuple of (region, clicks) for each user.  But because Java does
            // not support tuples out-of-the-box, we must use a custom class `RegionWithClicks` to
            // achieve the same effect.
            .leftJoin(userRegionsTable, new ValueJoiner<Long, String, RegionWithClicks>() {
                @Override
                public RegionWithClicks apply(Long clicks, String region) {
                    RegionWithClicks regionWithClicks = new RegionWithClicks(region == null ? "UNKNOWN" : region, clicks);
                    return regionWithClicks;
                }
            })
            // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
            .map(new KeyValueMapper<String, RegionWithClicks, KeyValue<String, Long>>() {
                @Override
                public KeyValue<String, Long> apply(String key, RegionWithClicks value) {
                    return new KeyValue<>(value.getRegion(), value.getClicks());
                }
            })
            // Compute the total per region by summing the individual click counts per region.
            .reduceByKey(new Reducer<Long>() {
                @Override
                public Long apply(Long value1, Long value2) {
                    return value1 + value2;
                }
            }, stringSerde, longSerde, "ClicksPerRegionUnwindowed");

        // Write the (continuously updating) results to the output topic.
        clicksPerRegion.to(stringSerde, longSerde, OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        // Wait briefly for the topology to be fully up and running (otherwise it might miss some or all
        // of the input data we produce below).
        Thread.sleep(5000);

        //
        // Step 2: Publish user-region information.
        //
        // To keep this code example simple and easier to understand/reason about, we publish all
        // user-region records before any user-click records (cf. step 3).  In practice though,
        // data records would typically be arriving concurrently in both input streams/topics.
        Properties userRegionsProducerConfig = new Properties();
        userRegionsProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        userRegionsProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        userRegionsProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        userRegionsProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        userRegionsProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        IntegrationTestUtils.produceKeyValuesSynchronously(USER_REGIONS_TOPIC, userRegions, userRegionsProducerConfig);

        //
        // Step 3: Publish some user click events.
        //
        Properties userClicksProducerConfig = new Properties();
        userClicksProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        userClicksProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        userClicksProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        userClicksProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        userClicksProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        IntegrationTestUtils.produceKeyValuesSynchronously(USER_CLICKS_TOPIC, userClicks, userClicksProducerConfig);

        // Give the stream processing application some time to do its work.
        Thread.sleep(10000);
        streams.close();

        //
        // Step 4: Verify the application's output data.
        //
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "join-integration-test-standard-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        List<KeyValue<String, Long>> actualClicksPerRegion = IntegrationTestUtils.readKeyValues(OUTPUT_TOPIC, consumerConfig);
        assertThat(actualClicksPerRegion, equalTo(expectedClicksPerRegion));
    }

}
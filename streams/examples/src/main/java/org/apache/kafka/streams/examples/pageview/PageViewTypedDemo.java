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
package org.apache.kafka.streams.examples.pageview;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Demonstrates how to perform a join between a KStream and a KTable, i.e. an example of a stateful computation,
 * using specific data types (here: JSON POJO; but can also be Avro specific bindings, etc.) for serdes
 * in Kafka Streams.
 *
 * In this example, we join a stream of pageviews (aka clickstreams) that reads from a topic named "streams-pageview-input"
 * with a user profile table that reads from a topic named "streams-userprofile-input", where the data format
 * is JSON string representing a record in the stream or table, to compute the number of pageviews per user region.
 *
 * Before running this example you must create the input topics and the output topic (e.g. via
 * bin/kafka-topics.sh --create ...), and write some data to the input topics (e.g. via
 * bin/kafka-console-producer.sh). Otherwise you won't see any data arriving in the output topic.
 */
public class PageViewTypedDemo {

    // POJO classes
    static public class PageView {
        public String user;
        public String page;
        public Long timestamp;
    }

    static public class UserProfile {
        public String region;
        public Long timestamp;
    }

    static public class PageViewByRegion {
        public String user;
        public String page;
        public String region;
    }

    static public class WindowedPageViewByRegion {
        public long windowStart;
        public String region;
    }

    static public class RegionCount {
        public long count;
        public String region;
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pageview-typed");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // TODO: the following can be removed with a serialization factory
        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<PageView> pageViewSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", PageView.class);
        pageViewSerializer.configure(serdeProps, false);

        final Deserializer<PageView> pageViewDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", PageView.class);
        pageViewDeserializer.configure(serdeProps, false);

        final Serde<PageView> pageViewSerde = Serdes.serdeFrom(pageViewSerializer, pageViewDeserializer);

        final Serializer<UserProfile> userProfileSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", UserProfile.class);
        userProfileSerializer.configure(serdeProps, false);

        final Deserializer<UserProfile> userProfileDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", UserProfile.class);
        userProfileDeserializer.configure(serdeProps, false);

        final Serde<UserProfile> userProfileSerde = Serdes.serdeFrom(userProfileSerializer, userProfileDeserializer);

        final Serializer<WindowedPageViewByRegion> wPageViewByRegionSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", WindowedPageViewByRegion.class);
        wPageViewByRegionSerializer.configure(serdeProps, false);

        final Deserializer<WindowedPageViewByRegion> wPageViewByRegionDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", WindowedPageViewByRegion.class);
        wPageViewByRegionDeserializer.configure(serdeProps, false);

        final Serde<WindowedPageViewByRegion> wPageViewByRegionSerde = Serdes.serdeFrom(wPageViewByRegionSerializer, wPageViewByRegionDeserializer);

        final Serializer<RegionCount> regionCountSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", RegionCount.class);
        regionCountSerializer.configure(serdeProps, false);

        final Deserializer<RegionCount> regionCountDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", RegionCount.class);
        regionCountDeserializer.configure(serdeProps, false);
        final Serde<RegionCount> regionCountSerde = Serdes.serdeFrom(regionCountSerializer, regionCountDeserializer);

        final Serializer<PageViewByRegion> pageViewByRegionSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", PageViewByRegion.class);
        pageViewByRegionSerializer.configure(serdeProps, false);
        final Deserializer<PageViewByRegion> pageViewByRegionDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", PageViewByRegion.class);
        pageViewByRegionDeserializer.configure(serdeProps, false);
        final Serde<PageViewByRegion> pageViewByRegionSerde = Serdes.serdeFrom(pageViewByRegionSerializer, pageViewByRegionDeserializer);

        KStream<String, PageView> views = builder.stream("streams-pageview-input", Consumed.with(Serdes.String(), pageViewSerde));

        KTable<String, UserProfile> users = builder.table("streams-userprofile-input",
                                                          Consumed.with(Serdes.String(), userProfileSerde));

        KStream<WindowedPageViewByRegion, RegionCount> regionCount = views
            .leftJoin(users, (view, profile) -> {
                PageViewByRegion viewByRegion = new PageViewByRegion();
                viewByRegion.user = view.user;
                viewByRegion.page = view.page;

                if (profile != null) {
                    viewByRegion.region = profile.region;
                } else {
                    viewByRegion.region = "UNKNOWN";
                }
                return viewByRegion;
            })
            .map((user, viewRegion) -> new KeyValue<>(viewRegion.region, viewRegion))
            .groupByKey(Serialized.with(Serdes.String(), pageViewByRegionSerde))
            .windowedBy(TimeWindows.of(TimeUnit.DAYS.toMillis(7)).advanceBy(TimeUnit.SECONDS.toMillis(1)))
            .count()
            .toStream()
            .map((key, value) -> {
                WindowedPageViewByRegion wViewByRegion = new WindowedPageViewByRegion();
                wViewByRegion.windowStart = key.window().start();
                wViewByRegion.region = key.key();

                RegionCount rCount = new RegionCount();
                rCount.region = key.key();
                rCount.count = value;

                return new KeyValue<>(wViewByRegion, rCount);
            });

        // write to the result topic
        regionCount.to("streams-pageviewstats-typed-output", Produced.with(wPageViewByRegionSerde, regionCountSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-pipe-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}

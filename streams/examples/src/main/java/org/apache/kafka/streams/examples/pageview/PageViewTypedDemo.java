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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates how to perform a join between a KStream and a KTable, i.e. an example of a stateful computation,
 * using specific data types (here: JSON POJO; but can also be Avro specific bindings, etc.) for serdes
 * in Kafka Streams.
 *
 * <p>In this example, we join a stream of pageviews (aka clickstreams) that reads from a topic named "streams-pageview-input"
 * with a user profile table that reads from a topic named "streams-userprofile-input", where the data format
 * is JSON string representing a record in the stream or table, to compute the number of pageviews per user region.
 *
 * <p>Before running this example you must create the input topics and the output topic (e.g. via
 * bin/kafka-topics --create ...), and write some data to the input topics (e.g. via
 * bin/kafka-console-producer). Otherwise you won't see any data arriving in the output topic.
 *
 * <p>The inputs for this example are:
 * - Topic: streams-pageview-input
 *   Key Format: (String) USER_ID
 *   Value Format: (JSON) {"_t": "pv", "user": (String USER_ID), "page": (String PAGE_ID), "timestamp": (long ms TIMESTAMP)}
 * <p>
 * - Topic: streams-userprofile-input
 *   Key Format: (String) USER_ID
 *   Value Format: (JSON) {"_t": "up", "region": (String REGION), "timestamp": (long ms TIMESTAMP)}
 *
 * <p>To observe the results, read the output topic (e.g., via bin/kafka-console-consumer)
 * - Topic: streams-pageviewstats-typed-output
 *   Key Format: (JSON) {"_t": "wpvbr", "windowStart": (long ms WINDOW_TIMESTAMP), "region": (String REGION)}
 *   Value Format: (JSON) {"_t": "rc", "count": (long REGION_COUNT), "region": (String REGION)}
 *
 * <p>Note, the "_t" field is necessary to help Jackson identify the correct class for deserialization in the
 * generic {@link JSONSerde}. If you instead specify a specific serde per class, you won't need the extra "_t" field.
 */
public class PageViewTypedDemo {

    /**
     * A serde for any class that implements {@link JSONSerdeCompatible}. Note that the classes also need to
     * be registered in the {@code @JsonSubTypes} annotation on {@link JSONSerdeCompatible}.
     *
     * @param <T> The concrete type of the class that gets de/serialized
     */
    public static class JSONSerde<T extends JSONSerdeCompatible> implements Serializer<T>, Deserializer<T>, Serde<T> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {}

        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(final String topic, final byte[] data) {
            if (data == null) {
                return null;
            }

            try {
                return (T) OBJECT_MAPPER.readValue(data, JSONSerdeCompatible.class);
            } catch (final IOException e) {
                throw new SerializationException(e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final T data) {
            if (data == null) {
                return null;
            }

            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (final Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public void close() {}

        @Override
        public Serializer<T> serializer() {
            return this;
        }

        @Override
        public Deserializer<T> deserializer() {
            return this;
        }
    }

    /**
     * An interface for registering types that can be de/serialized with {@link JSONSerde}.
     */
    @SuppressWarnings("DefaultAnnotationParam") // being explicit for the example
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
    @JsonSubTypes({
                      @JsonSubTypes.Type(value = PageView.class, name = "pv"),
                      @JsonSubTypes.Type(value = UserProfile.class, name = "up"),
                      @JsonSubTypes.Type(value = PageViewByRegion.class, name = "pvbr"),
                      @JsonSubTypes.Type(value = WindowedPageViewByRegion.class, name = "wpvbr"),
                      @JsonSubTypes.Type(value = RegionCount.class, name = "rc")
                  })
    public interface JSONSerdeCompatible {

    }

    // POJO classes
    static public class PageView implements JSONSerdeCompatible {
        public String user;
        public String page;
        public Long timestamp;
    }

    static public class UserProfile implements JSONSerdeCompatible {
        public String region;
        public Long timestamp;
    }

    static public class PageViewByRegion implements JSONSerdeCompatible {
        public String user;
        public String page;
        public String region;
    }

    static public class WindowedPageViewByRegion implements JSONSerdeCompatible {
        public long windowStart;
        public String region;
    }

    static public class RegionCount implements JSONSerdeCompatible {
        public long count;
        public String region;
    }

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pageview-typed");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JSONSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, PageView> views = builder.stream("streams-pageview-input", Consumed.with(Serdes.String(), new JSONSerde<>()));

        final KTable<String, UserProfile> users = builder.table("streams-userprofile-input", Consumed.with(Serdes.String(), new JSONSerde<>()));

        final Duration duration24Hours = Duration.ofHours(24);

        final KStream<WindowedPageViewByRegion, RegionCount> regionCount = views
            .leftJoin(users, (view, profile) -> {
                final PageViewByRegion viewByRegion = new PageViewByRegion();
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
            .groupByKey(Grouped.with(Serdes.String(), new JSONSerde<>()))
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(7), duration24Hours).advanceBy(Duration.ofSeconds(1)))
            .count()
            .toStream()
            .map((key, value) -> {
                final WindowedPageViewByRegion wViewByRegion = new WindowedPageViewByRegion();
                wViewByRegion.windowStart = key.window().start();
                wViewByRegion.region = key.key();

                final RegionCount rCount = new RegionCount();
                rCount.region = key.key();
                rCount.count = value;

                return new KeyValue<>(wViewByRegion, rCount);
            });

        // write to the result topic
        regionCount.to("streams-pageviewstats-typed-output", Produced.with(new JSONSerde<>(), new JSONSerde<>()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
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
        } catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}

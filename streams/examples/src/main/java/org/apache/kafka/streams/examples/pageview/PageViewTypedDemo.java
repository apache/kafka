/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Demonstrates how to perform a join between a KStream and a KTable, i.e. an example of a stateful computation,
 * using specific data types (here: JSON POJO; but can also be Avro specific bindings, etc.) for serdes
 * in Kafka Streams.
 *
 * In this example, we join a stream of pageviews (aka clickstreams) that reads from  a topic named "streams-pageview-input"
 * with a user profile table that reads from a topic named "streams-userprofile-input", where the data format
 * is JSON string representing a record in the stream or table, to compute the number of pageviews per user region.
 *
 * Before running this example you must create the source topic (e.g. via bin/kafka-topics.sh --create ...)
 * and write some data to it (e.g. via bin-kafka-console-producer.sh). Otherwise you won't see any data arriving in the output topic.
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

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pageview-typed");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

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

        KStream<String, PageView> views = builder.stream(Serdes.String(), pageViewSerde, "streams-pageview-input");

        KTable<String, UserProfile> users = builder.table(Serdes.String(), userProfileSerde, "streams-userprofile-input");

        KStream<WindowedPageViewByRegion, RegionCount> regionCount = views
                .leftJoin(users, new ValueJoiner<PageView, UserProfile, PageViewByRegion>() {
                    @Override
                    public PageViewByRegion apply(PageView view, UserProfile profile) {
                        PageViewByRegion viewByRegion = new PageViewByRegion();
                        viewByRegion.user = view.user;
                        viewByRegion.page = view.page;

                        if (profile != null) {
                            viewByRegion.region = profile.region;
                        } else {
                            viewByRegion.region = "UNKNOWN";
                        }
                        return viewByRegion;
                    }
                })
                .map(new KeyValueMapper<String, PageViewByRegion, KeyValue<String, PageViewByRegion>>() {
                    @Override
                    public KeyValue<String, PageViewByRegion> apply(String user, PageViewByRegion viewRegion) {
                        return new KeyValue<>(viewRegion.region, viewRegion);
                    }
                })
                .countByKey(TimeWindows.of("GeoPageViewsWindow", 7 * 24 * 60 * 60 * 1000L).advanceBy(1000), Serdes.String())
                // TODO: we can merge ths toStream().map(...) with a single toStream(...)
                .toStream()
                .map(new KeyValueMapper<Windowed<String>, Long, KeyValue<WindowedPageViewByRegion, RegionCount>>() {
                    @Override
                    public KeyValue<WindowedPageViewByRegion, RegionCount> apply(Windowed<String> key, Long value) {
                        WindowedPageViewByRegion wViewByRegion = new WindowedPageViewByRegion();
                        wViewByRegion.windowStart = key.window().start();
                        wViewByRegion.region = key.key();

                        RegionCount rCount = new RegionCount();
                        rCount.region = key.key();
                        rCount.count = value;

                        return new KeyValue<>(wViewByRegion, rCount);
                    }
                });

        // write to the result topic
        regionCount.to(wPageViewByRegionSerde, regionCountSerde, "streams-pageviewstats-typed-output");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(5000L);

        streams.close();
    }
}

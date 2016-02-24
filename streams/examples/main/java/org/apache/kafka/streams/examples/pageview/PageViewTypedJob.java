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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PageViewTypedJob {

    // POJO classes
    static public class PageView {
        public String user;
        public String page;
    }

    static public class UserProfile {
        public String region;
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
        props.put(StreamsConfig.JOB_ID_CONFIG, "streams-pageview");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonPOJOSerializer.class);
        props.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonPOJODeserializer.class);

        KStreamBuilder builder = new KStreamBuilder();

        final Serializer<String> stringSerializer = new StringSerializer();
        final Deserializer<String> stringDeserializer = new StringDeserializer();
        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();

        // TODO: the following can be removed with a serialization factory
        Map<String, Object> serdeProps = new HashMap<>();

        final Deserializer<PageView> pageViewDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", PageView.class);
        pageViewDeserializer.configure(serdeProps, false);

        final Deserializer<UserProfile> userProfileDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", UserProfile.class);
        pageViewDeserializer.configure(serdeProps, false);

        final Serializer<UserProfile> userProfileSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", UserProfile.class);
        userProfileSerializer.configure(serdeProps, false);

        final Serializer<WindowedPageViewByRegion> wPageViewByRegionSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", WindowedPageViewByRegion.class);
        userProfileSerializer.configure(serdeProps, false);

        final Serializer<RegionCount> regionCountSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", RegionCount.class);
        userProfileSerializer.configure(serdeProps, false);

        KStream<String, PageView> views = builder.stream(stringDeserializer, pageViewDeserializer, "streams-pageview-input");

        KStream<String, PageView> viewsByUser = views.map((dummy, record) -> new KeyValue<>(record.user, record));

        KTable<String, UserProfile> users = builder.table(stringSerializer, userProfileSerializer, stringDeserializer, userProfileDeserializer, "streams-userprofile-input");

        KStream<WindowedPageViewByRegion, RegionCount> regionCount = viewsByUser
                .leftJoin(users, (view, profile) -> {
                    PageViewByRegion viewByRegion = new PageViewByRegion();
                    viewByRegion.user = view.user;
                    viewByRegion.page = view.page;
                    viewByRegion.region = profile.region;

                    return viewByRegion;
                })
                .map((user, viewRegion) -> new KeyValue<>(viewRegion.region, viewRegion))
                .countByKey(HoppingWindows.of("GeoPageViewsWindow").with(7 * 24 * 60 * 60 * 1000),
                        stringSerializer, longSerializer,
                        stringDeserializer, longDeserializer)
                // TODO: we can merge ths toStream().map(...) with a single toStream(...)
                .toStream()
                .map(new KeyValueMapper<Windowed<String>, Long, KeyValue<WindowedPageViewByRegion, RegionCount>>() {
                    @Override
                    public KeyValue<WindowedPageViewByRegion, RegionCount> apply(Windowed<String> key, Long value) {
                        WindowedPageViewByRegion wViewByRegion = new WindowedPageViewByRegion();
                        wViewByRegion.windowStart = key.window().start();
                        wViewByRegion.region = key.value();

                        RegionCount rCount = new RegionCount();
                        rCount.region = key.value();
                        rCount.count = value;

                        return new KeyValue<>(wViewByRegion, rCount);
                    }
                });

        // write to the result topic
        regionCount.to("streams-pageviewstats-output", wPageViewByRegionSerializer, regionCountSerializer);

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
    }
}

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
import org.apache.kafka.streams.kstream.SerializationFactory;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.StreamsConfig;

import java.lang.Override;
import java.lang.reflect.Type;
import java.util.Properties;

public class PageViewTypedJob {

    // POJO classes
    static public class PageView {
        public String user;
        public String page;

        public PageView() {
        }

        public PageView(String user, String page) {
            this.user = user;
            this.page = page;
        }

    }

    static public class UserProfile {
        public String user;
        public String region;

        public UserProfile() {
        }

        public UserProfile(String user, String region) {
            this.user = user;
            this.region = region;
        }
    }

    static public class PageViewByRegion {
        public String user;
        public String page;
        public String region;

        public PageViewByRegion() {
        }

        public PageViewByRegion(String user, String page, String region) {
            this.user = user;
            this.page = page;
            this.region = region;
        }
    }

    static public class WindowedPageViewByRegion {
        public long windowStart;
        public String region;

        public WindowedPageViewByRegion() {

        }

        public WindowedPageViewByRegion(long windowStart, String region) {
            this.windowStart = windowStart;
            this.region = region;
        }
    }

    static public class RegionCount {
        public long count;
        public String region;
    }

    private static SerializationFactory serializationFactory = new SerializationFactory() {
        @Override
        public Serializer<?> getSerializer(Type type) {
            return (type instanceof Class) ? new JsonPOJOSerializer((Class) clazz) : null;
        }

        @Override
        public Deserializer<?> getDeserializer(Type type) {
            return (type instanceof Class) ? new JsonPOJODeserializer((Class) clazz) : null;
        }
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.JOB_ID_CONFIG, "streams-pageview");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");

        KStreamBuilder builder = new KStreamBuilder();

        //
        // register serializers/deserializers
        //
        builder.register(String.class, new StringSerializer(), new StringDeserializer());
        buidler.register(Long.class, new LongSerializer(), new LongDeserializer());
        builder.register(serializationFactory);

        //
        // define the topology
        //
        KStream<String, PageView> views = builder.stream("streams-pageview-input");

        KStream<String, PageView> viewsByUser = views.map((dummy, record) -> new KeyValue<>(record.user, record));

        KTable<String, UserProfile> users = builder.table("streams-userprofile-input");

        KStream<WindowedPageViewByRegion, RegionCount> regionCount = viewsByUser
                .leftJoin(users, (view, profile) -> {
                    return new PageViewByRegion(view.user, view.page, profile.region);
               })
                .map((user, viewRegion) -> new KeyValue<>(viewRegion.region, viewRegion))
                .countByKey(HoppingWindows.of("GeoPageViewsWindow").with(7 * 24 * 60 * 60 * 1000))
                .toStream()
                .map(new KeyValueMapper<Windowed<String>, Long, KeyValue<WindowedPageViewByRegion, RegionCount>>() {
                    @Override
                    public KeyValue<WindowedPageViewByRegion, RegionCount> apply(Windowed<String> key, Long value) {
                        WindowedPageViewByRegion wViewByRegion = new WindowedPageViewByRegion(key.window().start(), key.value());
                        RegionCount rCount = new RegionCount(key.value(), value);

                        return new KeyValue<>(wViewByRegion, rCount);
                    }
                });

        // write to the result topic
        regionCount.to("streams-pageviewstats-output");

        //
        // run the job
        //
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
    }
}

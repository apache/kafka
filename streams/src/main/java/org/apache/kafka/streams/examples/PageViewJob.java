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

package org.apache.kafka.streams.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Map;
import java.util.Properties;

public class PageViewJob {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamingConfig.JOB_ID_CONFIG, "streams-pageview");
        props.put(StreamingConfig.STATE_DIR_CONFIG, "/tmp/streams");
        props.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamingConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        StreamingConfig config = new StreamingConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        final Serializer<String> stringSerializer = new StringSerializer();
        final Deserializer<String> stringDeserializer = new StringDeserializer();

        KStream<String, JsonNode> views = builder.stream("streams-pageview-input");

        KStream<String, JsonNode> viewsByUser = views.map((dummy, record) -> new KeyValue<>(record.get("user").textValue(), record));

        KTable<String, JsonNode> users = builder.table("streams-userprofile-input");

        KTable<String, String> userRegions = users.mapValues(record -> record.get("region").textValue());

        KTable<Windowed<String>, String> regionCount = viewsByUser
                .leftJoin(userRegions, (view, region) -> {
                    ObjectNode jNode = JsonNodeFactory.instance.objectNode();

                    return jNode.put("user", view.get("user").textValue())
                            .put("page", view.get("page").textValue())
                            .put("region", region);
                })
                .map((user, viewRegion) -> new KeyValue<>(viewRegion.get("region").textValue(), viewRegion))
                .countByKey(HoppingWindows.of("GeoPageViewsWindow").with(7 * 24 * 60 * 60 * 1000), stringSerializer, stringDeserializer)
                .mapValues(new ValueMapper<Long, String>() {
                    @Override
                    public String apply(Long value) {
                        return value.toString();
                    }
                });

        // write to the result topic
        regionCount.to("streams-pageviewstats-output", new Serializer<Windowed<String>>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // do nothing
            }

            @Override
            public byte[] serialize(String topic, Windowed<String> data) {
                return stringSerializer.serialize(topic, data.value());
            }

            @Override
            public void close() {
                // do nothing
            }
        }, stringSerializer);

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}

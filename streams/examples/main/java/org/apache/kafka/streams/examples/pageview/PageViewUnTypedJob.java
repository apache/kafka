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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Properties;

public class PageViewUnTypedJob {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.JOB_ID_CONFIG, "streams-pageview");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        KStreamBuilder builder = new KStreamBuilder();

        final Serializer<String> stringSerializer = new StringSerializer();
        final Deserializer<String> stringDeserializer = new StringDeserializer();
        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();


        KStream<String, JsonNode> views = builder.stream("streams-pageview-input");

        KStream<String, JsonNode> viewsByUser = views.map((dummy, record) -> new KeyValue<>(record.get("user").textValue(), record));

        KTable<String, JsonNode> users = builder.table("streams-userprofile-input");

        KTable<String, String> userRegions = users.mapValues(record -> record.get("region").textValue());

        KStream<JsonNode, JsonNode> regionCount = viewsByUser
                .leftJoin(userRegions, (view, region) -> {
                    ObjectNode jNode = JsonNodeFactory.instance.objectNode();

                    return (JsonNode) jNode.put("user", view.get("user").textValue())
                            .put("page", view.get("page").textValue())
                            .put("region", region);
                })
                .map((user, viewRegion) -> new KeyValue<>(viewRegion.get("region").textValue(), viewRegion))
                .countByKey(HoppingWindows.of("GeoPageViewsWindow").with(7 * 24 * 60 * 60 * 1000),
                        stringSerializer, longSerializer,
                        stringDeserializer, longDeserializer)
                // TODO: we can merge ths toStream().map(...) with a single toStream(...)
                .toStream()
                .map(new KeyValueMapper<Windowed<String>, Long, KeyValue<JsonNode, JsonNode>>() {
                    @Override
                    public KeyValue<JsonNode, JsonNode> apply(Windowed<String> key, Long value) {
                        ObjectNode keyNode = JsonNodeFactory.instance.objectNode();
                        keyNode.put("window-start", key.window().start())
                                .put("region", key.window().start());

                        ObjectNode valueNode = JsonNodeFactory.instance.objectNode();
                        keyNode.put("count", value);

                        return new KeyValue<>((JsonNode) keyNode, (JsonNode) valueNode);
                    }
                });

        // write to the result topic
        regionCount.to("streams-pageviewstats-untyped-output");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
    }
}

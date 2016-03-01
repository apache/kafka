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
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Properties;

/**
 * Demonstrates how to perform a join between a KStream and a KTable, i.e. an example of a stateful computation,
 * using general data types (here: JSON; but can also be Avro generic bindings, etc.) for serdes
 * in Kafka Streams.
 *
 * In this example, we join a stream of pageviews (aka clickstreams) that reads from  a topic named "streams-pageview-input"
 * with a user profile table that reads from a topic named "streams-userprofile-input", where the data format
 * is JSON string representing a record in the stream or table, to compute the number of pageviews per user region.
 *
 * Before running this example you must create the source topic (e.g. via bin/kafka-topics.sh --create ...)
 * and write some data to it (e.g. via bin-kafka-console-producer.sh). Otherwise you won't see any data arriving in the output topic.
 */
public class PageViewUntypedJob {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.JOB_ID_CONFIG, "streams-pageview-untyped");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(StreamsConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        final Serializer<String> stringSerializer = new StringSerializer();
        final Deserializer<String> stringDeserializer = new StringDeserializer();
        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();

        KStream<String, JsonNode> views = builder.stream(stringDeserializer, jsonDeserializer, "streams-pageview-input");

        KTable<String, JsonNode> users = builder.table(stringSerializer, jsonSerializer, stringDeserializer, jsonDeserializer, "streams-userprofile-input");

        KTable<String, String> userRegions = users.mapValues(new ValueMapper<JsonNode, String>() {
            @Override
            public String apply(JsonNode record) {
                return record.get("region").textValue();
            }
        });

        KStream<JsonNode, JsonNode> regionCount = views
                .leftJoin(userRegions, new ValueJoiner<JsonNode, String, JsonNode>() {
                            @Override
                            public JsonNode apply(JsonNode view, String region) {
                                ObjectNode jNode = JsonNodeFactory.instance.objectNode();

                                return jNode.put("user", view.get("user").textValue())
                                        .put("page", view.get("page").textValue())
                                        .put("region", region == null ? "UNKNOWN" : region);
                            }
                        })
                .map(new KeyValueMapper<String, JsonNode, KeyValue<String, JsonNode>>() {
                    @Override
                    public KeyValue<String, JsonNode> apply(String user, JsonNode viewRegion) {
                        return new KeyValue<>(viewRegion.get("region").textValue(), viewRegion);
                    }
                })
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
                                .put("region", key.value());

                        ObjectNode valueNode = JsonNodeFactory.instance.objectNode();
                        valueNode.put("count", value);

                        return new KeyValue<>((JsonNode) keyNode, (JsonNode) valueNode);
                    }
                });

        // write to the result topic
        regionCount.to("streams-pageviewstats-untyped-output", jsonSerializer, jsonSerializer);

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // usually the streaming job would be ever running,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(5000L);

        streams.close();
    }
}

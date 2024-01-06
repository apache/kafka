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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.util.Properties;

/**
 * Demonstrates how to perform a join between a KStream and a KTable, i.e. an example of a stateful computation,
 * using general data types (here: JSON; but can also be Avro generic bindings, etc.) for serdes
 * in Kafka Streams.
 *
 * <p>In this example, we join a stream of pageviews (aka clickstreams) that reads from  a topic named "streams-pageview-input"
 * with a user profile table that reads from a topic named "streams-userprofile-input", where the data format
 * is JSON string representing a record in the stream or table, to compute the number of pageviews per user region.
 *
 * <p>Before running this example you must create the input topics and the output topic (e.g. via
 * bin/kafka-topics.sh --create ...), and write some data to the input topics (e.g. via
 * bin/kafka-console-producer.sh). Otherwise you won't see any data arriving in the output topic.
 */
public class PageViewUntypedDemo {

    public static void main(final String[] args) throws Exception {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pageview-untyped");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        final Consumed<String, JsonNode> consumed = Consumed.with(Serdes.String(), jsonSerde);
        final KStream<String, JsonNode> views = builder.stream("streams-pageview-input", consumed);

        final KTable<String, JsonNode> users = builder.table("streams-userprofile-input", consumed);

        final KTable<String, String> userRegions = users.mapValues(record -> record.get("region").textValue());

        final Duration duration24Hours = Duration.ofHours(24);

        final KStream<JsonNode, JsonNode> regionCount = views
            .leftJoin(userRegions, (view, region) -> {
                final ObjectNode jNode = JsonNodeFactory.instance.objectNode();
                return (JsonNode) jNode.put("user", view.get("user").textValue())
                        .put("page", view.get("page").textValue())
                        .put("region", region == null ? "UNKNOWN" : region);

            })
            .map((user, viewRegion) -> new KeyValue<>(viewRegion.get("region").textValue(), viewRegion))
            .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(7), duration24Hours).advanceBy(Duration.ofSeconds(1)))
            .count()
            .toStream()
            .map((key, value) -> {
                final ObjectNode keyNode = JsonNodeFactory.instance.objectNode();
                keyNode.put("window-start", key.window().start())
                        .put("region", key.key());

                final ObjectNode valueNode = JsonNodeFactory.instance.objectNode();
                valueNode.put("count", value);

                return new KeyValue<>((JsonNode) keyNode, (JsonNode) valueNode);
            });

        // write to the result topic
        regionCount.to("streams-pageviewstats-untyped-output", Produced.with(jsonSerde, jsonSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(5000L);

        streams.close();
    }
}

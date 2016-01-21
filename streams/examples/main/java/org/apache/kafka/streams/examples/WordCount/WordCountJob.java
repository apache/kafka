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

package org.apache.kafka.streams.examples.wordcount;

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
import org.apache.kafka.streams.kstream.Count;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.UnlimitedWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Arrays;
import java.util.Properties;

public class WordCountJob {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamingConfig.JOB_ID_CONFIG, "streams-wordcount");
        props.put(StreamingConfig.STATE_DIR_CONFIG, "/tmp/streams");
        props.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamingConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // can specify underlying client configs if necessary
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamingConfig config = new StreamingConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        final Serializer<String> stringSerializer = new StringSerializer();
        final Deserializer<String> stringDeserializer = new StringDeserializer();
        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();
        final Serializer<JsonNode> JsonSerializer = new JsonSerializer();

        KStream<String, String> source = builder.stream("streams-file-input");

        KStream<String, JsonNode> counts = source
                .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                    @Override
                    public Iterable<String> apply(String value) {
                        return Arrays.asList(value.toLowerCase().split(" "));
                    }
                }).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
                    @Override
                    public KeyValue<String, String> apply(String key, String value) {
                        return new KeyValue<String, String>(value, value);
                    }
                })
                .aggregateByKey(new Count<>(), UnlimitedWindows.of("Counts").startOn(0L),
                        stringSerializer, longSerializer,
                        stringDeserializer, longDeserializer)
                .toStream()
                .map(new KeyValueMapper<Windowed<String>, Long, KeyValue<String, JsonNode>>() {
                    @Override
                    public KeyValue<String, JsonNode> apply(Windowed<String> key, Long value) {
                        ObjectNode jNode = JsonNodeFactory.instance.objectNode();

                        jNode.put("word", key.value())
                                .put("count", value);

                        return new KeyValue<String, JsonNode>("", jNode);
                    }
                });

        counts.to("streams-wordcount-output", stringSerializer, JsonSerializer);

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}

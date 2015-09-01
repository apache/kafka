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

package org.apache.kafka.streaming.examples;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streaming.kstream.KStreamBuilder;
import org.apache.kafka.streaming.StreamingConfig;
import org.apache.kafka.streaming.KafkaStreaming;
import org.apache.kafka.streaming.kstream.KStream;
import org.apache.kafka.streaming.kstream.KeyValue;
import org.apache.kafka.streaming.kstream.KeyValueMapper;
import org.apache.kafka.streaming.kstream.Predicate;
import org.apache.kafka.streaming.processor.TimestampExtractor;

import java.util.HashMap;
import java.util.Map;

public class KStreamJob {

    public static void main(String[] args) throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
        props.put("timestamp.extractor", WallclockTimestampExtractor.class);
        StreamingConfig config = new StreamingConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        StringSerializer stringSerializer = new StringSerializer();
        IntegerSerializer intSerializer = new IntegerSerializer();

        KStream<String, String> stream1 = builder.from(new StringDeserializer(), new StringDeserializer(), "topic1");

        KStream<String, Integer> stream2 =
            stream1.map(new KeyValueMapper<String, String, KeyValue<String, Integer>>() {
                @Override
                public KeyValue<String, Integer> apply(String key, String value) {
                    return new KeyValue<>(key, new Integer(value));
                }
            }).filter(new Predicate<String, Integer>() {
                @Override
                public boolean apply(String key, Integer value) {
                    return true;
                }
            });

        KStream<String, Integer>[] streams = stream2.branch(
            new Predicate<String, Integer>() {
                @Override
                public boolean apply(String key, Integer value) {
                    return true;
                }
            },
            new Predicate<String, Integer>() {
                @Override
                public boolean apply(String key, Integer value) {
                    return true;
                }
            }
        );

        streams[0].sendTo("topic2", stringSerializer, intSerializer);
        streams[1].sendTo("topic3", stringSerializer, intSerializer);

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }

    public static class WallclockTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(String topic, Object key, Object value) {
            return System.currentTimeMillis();
        }
    }


}

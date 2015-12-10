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

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Properties;

public class KStreamJob {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamingConfig.JOB_ID_CONFIG, "example-kstream");
        props.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        StreamingConfig config = new StreamingConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> stream1 = builder.from("topic1");

        KStream<String, Integer> stream2 =
            stream1.map(new KeyValueMapper<String, String, KeyValue<String, Integer>>() {
                @Override
                public KeyValue<String, Integer> apply(String key, String value) {
                    return new KeyValue<>(key, new Integer(value));
                }
            }).filter(new Predicate<String, Integer>() {
                @Override
                public boolean test(String key, Integer value) {
                    return true;
                }
            });

        KStream<String, Integer>[] streams = stream2.branch(
            new Predicate<String, Integer>() {
                @Override
                public boolean test(String key, Integer value) {
                    return (value % 2) == 0;
                }
            },
            new Predicate<String, Integer>() {
                @Override
                public boolean test(String key, Integer value) {
                    return true;
                }
            }
        );

        streams[0].to("topic2");
        streams[1].to("topic3");

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}

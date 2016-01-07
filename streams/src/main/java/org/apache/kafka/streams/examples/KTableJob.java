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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.KeyValueToLongMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Properties;

public class KTableJob {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamingConfig.JOB_ID_CONFIG, "example-ktable");
        props.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        StreamingConfig config = new StreamingConfig(props);

        Serializer<String> stringSerializer = new StringSerializer();
        Deserializer<String> stringDeserializer = new StringDeserializer();

        KStreamBuilder builder = new KStreamBuilder();

        // stream aggregate
        KStream<String, Long> stream1 = builder.stream("topic1");

        @SuppressWarnings("unchecked")
        KTable<Windowed<String>, Long> wtable1 = stream1.sumByKey(new KeyValueToLongMapper<String, Long>() {
            @Override
            public long apply(String key, Long value) {
                return value;
            }
        }, HoppingWindows.of("window1").with(500L).every(500L).emit(1000L).until(1000L * 60 * 60 * 24 /* one day */), stringSerializer, stringDeserializer);

        // table aggregation
        KTable<String, String> table1 = builder.table("topic2");

        KTable<String, Long> table2 = table1.sum(new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(String key, String value) {
                return value;
            }
        }, new KeyValueToLongMapper<String, String>() {
            @Override
            public long apply(String key, String value) {
                return Long.parseLong(value);
            }
        }, stringSerializer, stringDeserializer, "table2");

        // stream-table join
        KStream<String, Long> stream2 = stream1.leftJoin(table2, new ValueJoiner<Long, Long, Long>() {
            @Override
            public Long apply(Long value1, Long value2) {
                if (value2 == null)
                    return 0L;
                else
                    return value1 * value2;
            }
        });

        // table-table join
        KTable<String, String> table3 = table1.outerJoin(table2, new ValueJoiner<String, Long, String>() {
            @Override
            public String apply(String value1, Long value2) {
                if (value2 == null)
                    return value1 + "-null";
                else if (value1 == null)
                    return "null-" + value2;
                else
                    return value1 + "-" + value2;
            }
        });

        KStream<String, Long> stream3 = wtable1.toStream(new ValueMapper<Windowed<String>, String>() {
            @Override
            public String apply(Windowed<String> windowedKey) {
                return windowedKey.value() + windowedKey.window().start();
            }
        });

        stream3.to("topic3");

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}

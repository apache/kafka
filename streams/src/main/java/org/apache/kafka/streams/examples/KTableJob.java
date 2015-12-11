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
import org.apache.kafka.streams.kstream.AggregateSupplier;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KWindowedTable;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.WindowMapper;

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

        KStreamBuilder builder = new KStreamBuilder();

        // stream aggregate
        KStream<String, Integer> stream1 = builder.stream("topic1");

        KWindowedTable<String, Integer, HoppingWindows> wtable1 = stream1.aggregateByKey(new AggregateSupplier<String, Integer, Integer>() {
            @Override
            public Aggregator<String, Integer, Integer> get() {
                return new Aggregator<String, Integer, Integer>() {
                    @Override
                    public Integer initialValue() {
                        return 0;
                    }

                    @Override
                    public Integer add(String aggKey, Integer value, Integer aggregate) {
                        return aggregate + value;
                    }

                    @Override
                    public Integer remove(String aggKey, Integer value, Integer aggregate) {
                        return aggregate - value;
                    }

                    @Override
                    public Integer merge(Integer aggr1, Integer aggr2) {
                        return aggr1 + aggr2;
                    }
                };
            }
        }, HoppingWindows.of(1000).every(500));

        // table aggregation
        KTable<String, String> table1 = builder.table("topic2");

        KTable<String, Integer> table2 = table1.aggregate(new AggregateSupplier<String, KeyValue<String, String>, Integer>() {
            @Override
            public Aggregator<String, KeyValue<String, String>, Integer> get() {
                return new Aggregator<String, KeyValue<String, String>, Integer>() {
                    @Override
                    public Integer initialValue() {
                        return 0;
                    }

                    @Override
                    public Integer add(String aggKey, KeyValue<String, String> value, Integer aggregate) {
                        return aggregate + new Integer(value.value);
                    }

                    @Override
                    public Integer remove(String aggKey, KeyValue<String, String> value, Integer aggregate) {
                        return aggregate - new Integer(value.value);
                    }

                    @Override
                    public Integer merge(Integer aggr1, Integer aggr2) {
                        return aggr1 + aggr2;
                    }
                };
            }
        }, new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(String key, String value) {
                return value;
            }
        });

        // stream-table join
        KStream<String, Integer> stream2 = stream1.leftJoin(table2, new ValueJoiner<Integer, Integer, Integer>() {
            @Override
            public Integer apply(Integer value1, Integer value2) {
                if (value2 == null)
                    return 0;
                else
                    return value1 * value2;
            }
        });

        // table-table join
        KTable<String, String> table3 = table1.outerJoin(table2, new ValueJoiner<String, Integer, String>() {
            @Override
            public String apply(String value1, Integer value2) {
                if (value2 == null)
                    return value1 + "-null";
                else if (value1 == null)
                    return "null-" + value2;
                else
                    return value1 + "-" + value2;
            }
        });

        // windowed table self join
        KWindowedTable<String, Integer, HoppingWindows> wtable2 = wtable1.leftJoin(wtable1, 1000 * 60 * 60 * 24 * 7 /* a week ago*/, new ValueJoiner<Integer, Integer, Integer>() {
            @Override
            public Integer apply(Integer value1, Integer value2) {
                if (value2 == null)
                    return value1;
                else
                    return value1 - value2;
            }
        });

        KStream<String, String> stream3 = wtable2.toStream(new WindowMapper<String, Integer, String, String>() {
            @Override
            public KeyValue<String, String> apply(String key, Integer value, Window window) {
                return new KeyValue<>(key + window.start(), value.toString());
            }
        });

        stream3.to("topic3");

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}

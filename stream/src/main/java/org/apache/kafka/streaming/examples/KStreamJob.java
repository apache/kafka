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
import org.apache.kafka.streaming.processor.ProcessorConfig;
import org.apache.kafka.streaming.KStreamProcess;
import org.apache.kafka.streaming.kstream.KStream;
import org.apache.kafka.streaming.kstream.KeyValue;
import org.apache.kafka.streaming.kstream.KeyValueMapper;
import org.apache.kafka.streaming.kstream.Predicate;

import java.util.Properties;

public class KStreamJob {

    public static void main(String[] args) throws Exception {
        ProcessorConfig config = new ProcessorConfig(new Properties());
        KStreamBuilder builder = new KStreamBuilder();

        StringSerializer stringSerializer = new StringSerializer();
        IntegerSerializer intSerializer = new IntegerSerializer();

        KStream<String, String> stream1 = builder.from(new StringDeserializer(), new StringDeserializer(), "topic1");

        KStream<String, Integer> stream2 =
            stream1.map(new KeyValueMapper<String, String, String, Integer>() {
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

        KStreamProcess kstream = new KStreamProcess(builder, config);
        kstream.run();
    }
}

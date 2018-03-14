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
package org.apache.kafka.streams.examples.wordcount;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.RocksDBStore;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

public class WordCountProcessorTest {
    StringDeserializer deserializer = new StringDeserializer();
    @Test
    public void test() throws Exception {
        KafkaStreams streams;

        streams = WordCountProcessorDemo.getStreamsClient(new String[]{"--run-with-old-store"});
        streams.start();

        streams.close();

        inspectCountsStore(false);
        streams = WordCountProcessorDemo.getStreamsClient(new String[]{"--run-with-upgrade-store", StreamsConfig.IN_PLACE_UPGRADE});
        streams.start();

        streams.close();

        inspectCountsStore(false);
        inspectCountsStore(true);

        streams = WordCountProcessorDemo.getStreamsClient(new String[]{"--run-with-upgrade-store"});
        streams.start();

        streams.close();

        inspectCountsStore(false);

        streams = WordCountProcessorDemo.getStreamsClient(new String[]{"--run-with-new-store"});
        streams.start();

        streams.close();

        inspectCountsStore(false);
    }

    @Test
    public void xxx() throws Exception {
        inspectCountsStore(false);
    }

    private void inspectCountsStore(boolean prepare) throws Exception {
        StreamsConfig config = new StreamsConfig(new Properties() {
            {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-processor");
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            }
        });
        RocksDBStore store = new RocksDBStore("Counts");
        store.openDB(new ProcessorContextImpl(
            null,
            null,
            config,
            null,
            new ProcessorStateManager(
                new TaskId(0, 0, prepare),
                Collections.singleton(new TopicPartition("streams-plaintext-input", 0)),
                prepare,
                new StateDirectory(config, Time.SYSTEM),
                null,
                null,
                false,
                new LogContext()
            ),
            null,
            null
        ));

        KeyValueIterator<Bytes, byte[]> iter = store.all();
        int count = 0;
        while (iter.hasNext()) {
            ++count;
            KeyValue<Bytes, byte[]> record = iter.next();
            System.out.println(deserializer.deserialize(null, record.key.get()) + " : " + record.value.length);
        }
        System.out.println("# " + count);

        iter.close();
        store.close();

    }
}

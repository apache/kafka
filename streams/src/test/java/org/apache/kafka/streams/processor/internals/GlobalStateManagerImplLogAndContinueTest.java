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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockSourceNode;
import org.apache.kafka.test.NoOpReadOnlyStore;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GlobalStateManagerImplLogAndContinueTest extends GlobalStateManagerImplTest {

    @Before
    public void before() {
        final Map<String, String> storeToTopic = new HashMap<>();

        storeToTopic.put(storeName1, t1.topic());
        storeToTopic.put(storeName2, t2.topic());
        storeToTopic.put(storeName3, t3.topic());
        storeToTopic.put(storeName4, t4.topic());
        storeToTopic.put(storeName5, t5.topic());
        storeToTopic.put(storeName6, t6.topic());

        store1 = new NoOpReadOnlyStore<>(storeName1, true);
        store2 = new ConverterStore<>(storeName2, true);
        store3 = new NoOpReadOnlyStore<>(storeName3);
        store4 = new NoOpReadOnlyStore<>(storeName4);
        store5 = new NoOpReadOnlyStore<>(storeName5, true);
        store6 = new NoOpReadOnlyStore<>(storeName6, true);

        final Deserializer<Long> longDeserializer = Serdes.Long().deserializer();
        final MockSourceNode<Long, Long> source1 = new MockSourceNode<>(new String[]{t5.topic()}, longDeserializer,
            longDeserializer);
        final MockSourceNode<Long, Long> source2 = new MockSourceNode<>(new String[]{t6.topic()}, longDeserializer,
            longDeserializer);

        topology = withGlobalStores(mkMap(mkEntry(t5.topic(), source1), mkEntry(t6.topic(), source2)),
            asList(store1, store2, store3, store4, store5, store6),
            storeToTopic);

        streamsConfig = new StreamsConfig(new Properties() {
            {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
                put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
            }
        });
        stateDirectory = new StateDirectory(streamsConfig, time, true);
        consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        stateManager = new GlobalStateManagerImpl(
                new LogContext("test"),
                topology,
                consumer,
                stateDirectory,
                stateRestoreListener,
                streamsConfig);
        processorContext = new InternalMockProcessorContext(stateDirectory.globalStateDir(), streamsConfig);
        stateManager.setGlobalProcessorContext(processorContext);
        checkpointFile = new File(stateManager.baseDir(), StateManagerUtil.CHECKPOINT_FILE_NAME);
    }

    @Test
    public void shouldThrowStreamsExceptionWhenRestoringWithLogAndFailExceptionHandler() {
        return;
    }

    @Test
    public void shouldNotThrowStreamsExceptionWhenRestoringWithLogAndContinueExceptionHandler() {
        final HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        startOffsets.put(t5, 1L);
        final HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(t5, 3L);
        consumer.updatePartitions(t5.topic(), Collections.singletonList(new PartitionInfo(t5.topic(), t5.partition(), null, null, null)));
        consumer.assign(Collections.singletonList(t5));
        consumer.updateEndOffsets(endOffsets);
        consumer.updateBeginningOffsets(startOffsets);
        final byte[] specialString = "specialKey".getBytes(StandardCharsets.UTF_8);
        final byte[] longValue = longToBytes(1);

        consumer.addRecord(new ConsumerRecord<>(t5.topic(), t5.partition(), 1, longValue, longValue));
        consumer.addRecord(new ConsumerRecord<>(t5.topic(), t5.partition(), 2, specialString, specialString));
        consumer.addRecord(new ConsumerRecord<>(t5.topic(), t5.partition(), 3, longValue, longValue));

        stateManager.initialize();
        stateManager.register(store5, stateRestoreCallback);

        assertEquals(2, stateRestoreCallback.restored.size());

    }

    @Test
    public void shouldProcessTombstoneRecords() {
        final HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        startOffsets.put(t5, 1L);
        final HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(t5, 3L);
        consumer.updatePartitions(t5.topic(), Collections.singletonList(new PartitionInfo(t5.topic(), t5.partition(), null, null, null)));
        consumer.assign(Collections.singletonList(t5));
        consumer.updateEndOffsets(endOffsets);
        consumer.updateBeginningOffsets(startOffsets);
        final byte[] specialString = "specialKey".getBytes(StandardCharsets.UTF_8);
        final byte[] longValue = longToBytes(1);

        consumer.addRecord(new ConsumerRecord<>(t5.topic(), t5.partition(), 1, longValue, longValue));
        consumer.addRecord(new ConsumerRecord<>(t5.topic(), t5.partition(), 2, specialString, specialString));
        consumer.addRecord(new ConsumerRecord<>(t5.topic(), t5.partition(), 3, longValue, null));

        stateManager.initialize();
        stateManager.register(store5, stateRestoreCallback);

        assertEquals(2, stateRestoreCallback.restored.size());
        assertNull(store5.get(1L));

    }

}

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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.internals.PositionSerde;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashSet;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.processor.internals.InternalProcessorContext.BYTEARRAY_VALUE_SERIALIZER;
import static org.apache.kafka.streams.processor.internals.InternalProcessorContext.BYTES_KEY_SERIALIZER;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class WriteConsistencyVectorTest {

    @Mock
    private StreamsConfig streamsConfig;
    @Mock
    private ProcessorStateManager stateManager;
    @Mock
    private RecordCollector recordCollector;
    @Mock
    private StreamTask task;
    @Mock
    private TaskId taskId;
    @Mock
    private StreamsMetricsImpl streamsMetrics;
    @Mock
    private ThreadCache threadCache;
    private ProcessorContextImpl context;

    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    private static final long VALUE = 42L;
    private static final byte[] VALUE_BYTES = String.valueOf(VALUE).getBytes();
    private static final long TIMESTAMP = 21L;
    private static final String REGISTERED_STORE_NAME = "registered-store";
    private static final TopicPartition CHANGELOG_PARTITION = new TopicPartition("store-changelog", 1);
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final Integer INPUT_PARTITION = 0;
    private static final Long INPUT_OFFSET = 100L;

    @Before
    public void setup() {
        when(streamsConfig.originals()).thenReturn(Collections.singletonMap(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true));
        when(streamsConfig.values()).thenReturn(Collections.emptyMap());
        when(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).thenReturn("add-id");

        when(stateManager.taskType()).thenReturn(TaskType.ACTIVE);
        when(stateManager.registeredChangelogPartitionFor(REGISTERED_STORE_NAME)).thenReturn(CHANGELOG_PARTITION);

        context = new ProcessorContextImpl(
                taskId,
                streamsConfig,
                stateManager,
                streamsMetrics,
                threadCache
        );

        context.transitionToActive(task, null, null);

        context.setCurrentNode(
                new ProcessorNode<>(
                        "fake",
                        (org.apache.kafka.streams.processor.api.Processor<String, Long, Object, Object>) null,
                        new HashSet<>(
                                asList(
                                        "LocalKeyValueStore",
                                        "LocalTimestampedKeyValueStore",
                                        "LocalWindowStore",
                                        "LocalTimestampedWindowStore",
                                        "LocalSessionStore"
                                )
                        )
                )
        );
    }

    @Test
    public void shouldSendConsistencyVectorToChangelogTopic() {
        final Position position = Position.emptyPosition();
        position.withComponent(INPUT_TOPIC_NAME, INPUT_PARTITION, INPUT_OFFSET);
        context.setRecordContext(new ProcessorRecordContext(-1, INPUT_OFFSET, INPUT_PARTITION, INPUT_TOPIC_NAME, new RecordHeaders()));
        final Headers headers = new RecordHeaders();
        headers.add(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position).array()));

        context.transitionToActive(task, recordCollector, null);

        context.logChange(REGISTERED_STORE_NAME, KEY_BYTES, VALUE_BYTES, TIMESTAMP, position);

        verify(recordCollector).send(
                CHANGELOG_PARTITION.topic(),
                KEY_BYTES,
                VALUE_BYTES,
                headers,
                CHANGELOG_PARTITION.partition(),
                TIMESTAMP,
                BYTES_KEY_SERIALIZER,
                BYTEARRAY_VALUE_SERIALIZER,
                null,
                null);
    }
}

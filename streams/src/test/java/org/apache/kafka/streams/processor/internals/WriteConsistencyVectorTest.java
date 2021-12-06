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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.internals.PositionSerde;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.processor.internals.InternalProcessorContext.BYTEARRAY_VALUE_SERIALIZER;
import static org.apache.kafka.streams.processor.internals.InternalProcessorContext.BYTES_KEY_SERIALIZER;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class WriteConsistencyVectorTest {

    private ProcessorContextImpl context;
    private final StreamsConfig streamsConfig = streamsConfigMock();
    private final RecordCollector recordCollector = mock(RecordCollector.class);

    private static final String KEY = "key";
    private static final Bytes KEY_BYTES = Bytes.wrap(KEY.getBytes());
    private static final long VALUE = 42L;
    private static final byte[] VALUE_BYTES = String.valueOf(VALUE).getBytes();
    private static final long TIMESTAMP = 21L;
    private static final long STREAM_TIME = 50L;
    private static final String REGISTERED_STORE_NAME = "registered-store";
    private static final TopicPartition CHANGELOG_PARTITION = new TopicPartition("store-changelog", 1);
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final Integer INPUT_PARTITION = 0;
    private static final Long INPUT_OFFSET = 100L;

    @Before
    public void setup() {
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);
        expect(stateManager.taskType()).andStubReturn(TaskType.ACTIVE);
        expect(stateManager.registeredChangelogPartitionFor(REGISTERED_STORE_NAME)).andStubReturn(CHANGELOG_PARTITION);
        replay(stateManager);

        context = new ProcessorContextImpl(
                mock(TaskId.class),
                streamsConfig,
                stateManager,
                mock(StreamsMetricsImpl.class),
                mock(ThreadCache.class)
        );

        final StreamTask task = mock(StreamTask.class);
        expect(task.streamTime()).andReturn(STREAM_TIME);
        EasyMock.expect(task.recordCollector()).andStubReturn(recordCollector);
        replay(task);
        ((InternalProcessorContext) context).transitionToActive(task, null, null);

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
        recordCollector.send(
                CHANGELOG_PARTITION.topic(),
                KEY_BYTES,
                VALUE_BYTES,
                headers,
                CHANGELOG_PARTITION.partition(),
                TIMESTAMP,
                BYTES_KEY_SERIALIZER,
                BYTEARRAY_VALUE_SERIALIZER
        );

        final StreamTask task = EasyMock.createNiceMock(StreamTask.class);

        replay(recordCollector, task);
        context.transitionToActive(task, recordCollector, null);

        context.logChange(REGISTERED_STORE_NAME, KEY_BYTES, VALUE_BYTES, TIMESTAMP, position);

        verify(recordCollector);
    }

    private StreamsConfig streamsConfigMock() {
        final StreamsConfig streamsConfig = mock(StreamsConfig.class);

        final Map<String, Object> myValues = new HashMap<>();
        myValues.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        expect(streamsConfig.originals()).andStubReturn(myValues);
        expect(streamsConfig.values()).andStubReturn(Collections.emptyMap());
        expect(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andStubReturn("add-id");
        expect(streamsConfig.defaultValueSerde()).andStubReturn(Serdes.ByteArray());
        expect(streamsConfig.defaultKeySerde()).andStubReturn(Serdes.ByteArray());
        replay(streamsConfig);
        return streamsConfig;
    }
}

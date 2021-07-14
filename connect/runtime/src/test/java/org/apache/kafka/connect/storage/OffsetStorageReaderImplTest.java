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
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
public class OffsetStorageReaderImplTest {

    private static final String NAMESPACE = "reddit-source";
    private static final Map<String, Object> PARTITION1 = Collections.singletonMap("subreddit", "apachekafka");
    private static final Map<String, Object> PARTITION2 = Collections.singletonMap("subreddit", "CatsStandingUp");

    private static final Map<String, Object> OFFSET1 = Collections.singletonMap("timestamp", "4761");
    private static final Map<String, Object> OFFSET2 = Collections.singletonMap("timestamp", "2112");

    @Mock private Converter keyConverter;
    @Mock private Converter valueConverter;
    @Mock private ConnectorOffsetBackingStore store;

    private OffsetStorageReaderImpl offsetReader;

    @Before
    public void setup() {
        offsetReader = new OffsetStorageReaderImpl(store, NAMESPACE, keyConverter, valueConverter);
    }

    @Test
    public void testSingleRead() {
        expectOffsetsForPartitions(Collections.singletonMap(PARTITION1, OFFSET1));

        PowerMock.replayAll();

        assertEquals(
                OFFSET1,
                offsetReader.offset(PARTITION1)
        );
    }

    @Test
    public void testMultipleReads() {
        Map<Map<String, Object>, Map<String, Object>> expectedOffsets = new HashMap<>();
        expectedOffsets.put(PARTITION1, OFFSET1);
        expectedOffsets.put(PARTITION2, OFFSET2);
        expectOffsetsForPartitions(expectedOffsets);

        PowerMock.replayAll();

        assertEquals(
                expectedOffsets,
                offsetReader.offsets(expectedOffsets.keySet())
        );
    }

    private void expectOffsetsForPartitions(Map<Map<String, Object>, Map<String, Object>> offsetsAndPartitions) {
        Map<Map<String, Object>, byte[]> serializedPartitions = offsetsAndPartitions.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), this::serialize));

        serializedPartitions.forEach((partition, serializedPartition) ->
            EasyMock.expect(keyConverter.fromConnectData(NAMESPACE, null, Arrays.asList(NAMESPACE, partition)))
                    .andReturn(serializedPartition)
        );

        Map<Map<String, Object>, byte[]> serializedOffsets = offsetsAndPartitions.values().stream()
                .collect(Collectors.toMap(Function.identity(), this::serialize));

        Map<ByteBuffer, ByteBuffer> serializedOffsetsAndPartitions = serializedPartitions.entrySet().stream()
                .collect(Collectors.toMap(e -> ByteBuffer.wrap(e.getValue()), e -> ByteBuffer.wrap(serializedOffsets.get(offsetsAndPartitions.get(e.getKey())))));

        EasyMock.expect(store.get(EasyMock.anyObject())).andReturn(CompletableFuture.completedFuture(serializedOffsetsAndPartitions));

        serializedOffsets.forEach((offset, serializedOffset) ->
            EasyMock.expect(valueConverter.toConnectData(NAMESPACE, serializedOffset)).andReturn(new SchemaAndValue(null, offset))
        );
    }

    private byte[] serialize(Map<String, Object> partitionOrOffset) {
        return ByteBuffer.allocate(4).putInt(partitionOrOffset.hashCode()).array();
    }
}

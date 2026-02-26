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
package org.apache.kafka.streams.state.internals;


import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
abstract class AbstractColumnFamilyAccessorTest {

    @Mock
    protected ColumnFamilyHandle offsetsCF;

    @Mock
    protected RocksDBStore.DBAccessor dbAccessor;

    protected AbstractColumnFamilyAccessor accessor;

    abstract AbstractColumnFamilyAccessor createColumnFamilyAccessor();
    private final LongSerializer offsetSerializer = new LongSerializer();
    private final StringSerializer topicSerializer = new StringSerializer();


    @BeforeEach
    public void setUp() {
        accessor = createColumnFamilyAccessor();
    }

    @Test
    public void shouldCommitOffsets() throws RocksDBException {
        final TopicPartition tp0 = new TopicPartition("testTopic", 0);
        final TopicPartition tp1 = new TopicPartition("testTopic", 1);
        final Map<TopicPartition, Long> changelogOffsets = Map.of(tp0, 10L, tp1, 20L);
        accessor.commit(dbAccessor, changelogOffsets);
        verify(dbAccessor).flush(any(ColumnFamilyHandle[].class));
        verify(dbAccessor).put(eq(offsetsCF), eq(topicSerializer.serialize(null, tp0.toString())), eq(offsetSerializer.serialize(null, 10L)));
        verify(dbAccessor).put(eq(offsetsCF), eq(topicSerializer.serialize(null, tp1.toString())), eq(offsetSerializer.serialize(null, 20L)));
    }

}
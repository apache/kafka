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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class IncompleteBatchesTest {

    @Mock
    private MemoryRecordsBuilder recordsBuilder;
    @Mock
    private CompressionType compressionType;

    private TopicPartition topicPartition;
    private ProducerBatch splittedBatch;
    private ProducerBatch nonSplittedBatch;
    private final long createdMs = 0;

    @Before
    public void setUp() {
        when(recordsBuilder.compressionType()).thenReturn(compressionType);
        topicPartition = new TopicPartition("test", 0);
        final boolean isSplitBatch = true;
        final boolean isNotSplitBatch = false;
        splittedBatch = new ProducerBatch(topicPartition, recordsBuilder, createdMs, isSplitBatch);
        nonSplittedBatch = new ProducerBatch(topicPartition, recordsBuilder, createdMs, isNotSplitBatch);
    }

    @Test
    public void testCopyAll() {
        final int expectedBatches = 2;
        final IncompleteBatches incompleteBatches = new IncompleteBatches();
        incompleteBatches.add(splittedBatch);
        incompleteBatches.add(nonSplittedBatch);

        final List<ProducerBatch> batches = incompleteBatches.copyAll();

        assertThat(batches, hasItems(splittedBatch, nonSplittedBatch));
        assertThat(expectedBatches, is(batches.size()));
    }

    @Test
    public void testAdd() {
        final IncompleteBatches incompleteBatches = new IncompleteBatches();

        incompleteBatches.add(nonSplittedBatch);

        assertThat(incompleteBatches.isEmpty(), is(false));
    }

    @Test
    public void testRemove() {
        final IncompleteBatches incompleteBatches = new IncompleteBatches();
        incompleteBatches.add(nonSplittedBatch);

        assertThat(incompleteBatches.isEmpty(), is(false));

        incompleteBatches.remove(nonSplittedBatch);

        assertThat(incompleteBatches.isEmpty(), is(true));
    }

    @Test(expected = IllegalStateException.class)
    public void testRemoveShouldThrowAIllegalStateException() {
        final IncompleteBatches incompleteBatches = new IncompleteBatches();
        incompleteBatches.add(nonSplittedBatch);

        incompleteBatches.remove(splittedBatch);
    }

    @Test
    public void testIsEmptyShouldBeTrue() {
        final IncompleteBatches incompleteBatches = new IncompleteBatches();

        assertThat(incompleteBatches.isEmpty(), is(true));
    }

    @Test
    public void testIsEmptyShouldBeFalse() {
        final IncompleteBatches incompleteBatches = new IncompleteBatches();
        incompleteBatches.add(nonSplittedBatch);

        assertThat(incompleteBatches.isEmpty(), is(false));
    }
}

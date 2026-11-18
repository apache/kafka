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

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.testutil.DummyStreamsConfig;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TopologyMetadataTest {
    static final String TOPOLOGY1 = "topology1";
    static final String TOPOLOGY2 = "topology2";

    @Test
    public void testPauseResume() {
        final InternalTopologyBuilder internalTopologyBuilder = mock(InternalTopologyBuilder.class);
        final StreamsConfig config = new DummyStreamsConfig();

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder,
            config);

        assertFalse(topologyMetadata.isPaused(TOPOLOGY1));
        assertFalse(topologyMetadata.isPaused(TOPOLOGY2));

        topologyMetadata.pauseTopology(TOPOLOGY1);
        assertTrue(topologyMetadata.isPaused(TOPOLOGY1));
        assertFalse(topologyMetadata.isPaused(TOPOLOGY2));

        topologyMetadata.resumeTopology(TOPOLOGY1);
        assertFalse(topologyMetadata.isPaused(TOPOLOGY1));
        assertFalse(topologyMetadata.isPaused(TOPOLOGY2));
    }

    @Test
    public void testSubtopologyCompare() {
        Subtopology subtopology1 = new Subtopology(0, "1");
        Subtopology subtopology2 = new Subtopology(1, "1");

        assertTrue(subtopology1.compareTo(subtopology2) < 0);

        subtopology1 = new Subtopology(0, null);
        subtopology2 = new Subtopology(0, null);
        assertEquals(0, subtopology1.compareTo(subtopology2));

        subtopology1 = new Subtopology(0, null);
        subtopology2 = new Subtopology(0, "1");
        assertTrue(subtopology1.compareTo(subtopology2) < 0);

        subtopology1 = new Subtopology(0, "1");
        subtopology2 = new Subtopology(0, null);
        assertTrue(subtopology1.compareTo(subtopology2) > 0);

        subtopology1 = new Subtopology(0, "1");
        subtopology2 = new Subtopology(0, "2");
        assertTrue(subtopology1.compareTo(subtopology2) < 0);
    }

    @Test
    public void testLockFailureIsNotMaskedByUnlock() throws Exception {
        final RuntimeException lockFailure = new RuntimeException("lock failed");
        final TopologyMetadata topologyMetadata = new TopologyMetadata(new ConcurrentSkipListMap<>(), new DummyStreamsConfig());
        final TopologyMetadata.TopologyVersion failingVersion = new TopologyMetadata.TopologyVersion();
        failingVersion.topologyLock = new ReentrantLock() {
            @Override
            public void lock() {
                throw lockFailure;
            }
        };
        TestUtils.setFieldValue(topologyMetadata, "version", failingVersion);

        final List<Executable> operations = List.of(
            () -> topologyMetadata.executeTopologyUpdatesAndBumpThreadVersion(added -> { }, removed -> { }),
            topologyMetadata::maybeNotifyTopologyVersionListeners,
            topologyMetadata::wakeupThreads,
            () -> topologyMetadata.maybeWaitForNonEmptyTopology(() -> StreamThread.State.RUNNING),
            () -> topologyMetadata.registerAndBuildNewTopology(new KafkaFutureImpl<>(), mock(InternalTopologyBuilder.class)),
            () -> topologyMetadata.unregisterTopology(new KafkaFutureImpl<>(), TOPOLOGY1)
        );
        for (final Executable operation : operations)
            assertSame(lockFailure, assertThrows(RuntimeException.class, operation));
    }
}

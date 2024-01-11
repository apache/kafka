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

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.testutil.DummyStreamsConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;


@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TopologyMetadataTest {
    final static String TOPOLOGY1 = "topology1";
    final static String TOPOLOGY2 = "topology2";

    @Test
    public void testPauseResume() {
        final InternalTopologyBuilder internalTopologyBuilder = mock(InternalTopologyBuilder.class);
        final StreamsConfig config = new DummyStreamsConfig();

        final TopologyMetadata topologyMetadata = new TopologyMetadata(internalTopologyBuilder,
            config);

        Assert.assertFalse(topologyMetadata.isPaused(TOPOLOGY1));
        Assert.assertFalse(topologyMetadata.isPaused(TOPOLOGY2));

        topologyMetadata.pauseTopology(TOPOLOGY1);
        assertTrue(topologyMetadata.isPaused(TOPOLOGY1));
        Assert.assertFalse(topologyMetadata.isPaused(TOPOLOGY2));

        topologyMetadata.resumeTopology(TOPOLOGY1);
        Assert.assertFalse(topologyMetadata.isPaused(TOPOLOGY1));
        Assert.assertFalse(topologyMetadata.isPaused(TOPOLOGY2));
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
}

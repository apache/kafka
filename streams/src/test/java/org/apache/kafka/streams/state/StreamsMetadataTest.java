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

package org.apache.kafka.streams.state;

import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.Assert.assertTrue;

public class StreamsMetadataTest {

    private static final HostInfo HOST_INFO = new HostInfo("local", 12);
    private static final TopicPartition TP_0 = new TopicPartition("t", 0);
    private static final TopicPartition TP_1 = new TopicPartition("t", 1);

    private StreamsMetadata streamsMetadata;

    @Before
    public void setUp() {
        streamsMetadata = new StreamsMetadata(
            HOST_INFO,
            mkSet("store1", "store2"),
            mkSet(TP_0, TP_1),
            mkSet("store2"),
            mkSet(TP_1)
        );
    }

    @Test
    public void shouldNotAllowModificationOfInternalStateViaGetters() {
        assertTrue(isUnmodifiable(streamsMetadata.stateStoreNames()));
        assertTrue(isUnmodifiable(streamsMetadata.topicPartitions()));
        assertTrue(isUnmodifiable(streamsMetadata.standbyTopicPartitions()));
        assertTrue(isUnmodifiable(streamsMetadata.standbyStateStoreNames()));
    }

    private static boolean isUnmodifiable(final Collection<?> collection) {
        try {
            collection.clear();
            return false;
        } catch (final UnsupportedOperationException e) {
            return true;
        }
    }
}
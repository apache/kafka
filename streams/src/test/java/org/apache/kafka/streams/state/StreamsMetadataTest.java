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
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.internals.StreamsMetadataImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class StreamsMetadataTest {

    private static final HostInfo HOST_INFO = new HostInfo("local", 12);
    public static final Set<String> STATE_STORE_NAMES = mkSet("store1", "store2");
    private static final TopicPartition TP_0 = new TopicPartition("t", 0);
    private static final TopicPartition TP_1 = new TopicPartition("t", 1);
    public static final Set<TopicPartition> TOPIC_PARTITIONS = mkSet(TP_0, TP_1);
    public static final Set<String> STAND_BY_STORE_NAMES = mkSet("store2");
    public static final Set<TopicPartition> STANDBY_TOPIC_PARTITIONS = mkSet(TP_1);

    private StreamsMetadata streamsMetadata;

    @Before
    public void setUp() {
        streamsMetadata = new StreamsMetadataImpl(
                HOST_INFO,
                STATE_STORE_NAMES,
                TOPIC_PARTITIONS,
                STAND_BY_STORE_NAMES,
                STANDBY_TOPIC_PARTITIONS
        );
    }

    @Test
    public void shouldNotAllowModificationOfInternalStateViaGetters() {
        assertTrue(isUnmodifiable(streamsMetadata.stateStoreNames()));
        assertTrue(isUnmodifiable(streamsMetadata.topicPartitions()));
        assertTrue(isUnmodifiable(streamsMetadata.standbyTopicPartitions()));
        assertTrue(isUnmodifiable(streamsMetadata.standbyStateStoreNames()));
    }

    @Test
    public void shouldFollowHashCodeAndEqualsContract() {
        final StreamsMetadata same = new StreamsMetadataImpl(
                HOST_INFO,
                STATE_STORE_NAMES,
                TOPIC_PARTITIONS,
                STAND_BY_STORE_NAMES,
                STANDBY_TOPIC_PARTITIONS);
        assertEquals(streamsMetadata, same);
        assertEquals(streamsMetadata.hashCode(), same.hashCode());

        final StreamsMetadata differHostInfo = new StreamsMetadataImpl(
                new HostInfo("different", 122),
                STATE_STORE_NAMES,
                TOPIC_PARTITIONS,
                STAND_BY_STORE_NAMES,
                STANDBY_TOPIC_PARTITIONS);
        assertNotEquals(streamsMetadata, differHostInfo);
        assertNotEquals(streamsMetadata.hashCode(), differHostInfo.hashCode());

        final StreamsMetadata differStateStoreNames = new StreamsMetadataImpl(
                HOST_INFO,
                mkSet("store1"),
                TOPIC_PARTITIONS,
                STAND_BY_STORE_NAMES,
                STANDBY_TOPIC_PARTITIONS);
        assertNotEquals(streamsMetadata, differStateStoreNames);
        assertNotEquals(streamsMetadata.hashCode(), differStateStoreNames.hashCode());

        final StreamsMetadata differTopicPartitions = new StreamsMetadataImpl(
                HOST_INFO,
                STATE_STORE_NAMES,
                mkSet(TP_0),
                STAND_BY_STORE_NAMES,
                STANDBY_TOPIC_PARTITIONS);
        assertNotEquals(streamsMetadata, differTopicPartitions);
        assertNotEquals(streamsMetadata.hashCode(), differTopicPartitions.hashCode());

        final StreamsMetadata differStandByStores = new StreamsMetadataImpl(
                HOST_INFO,
                STATE_STORE_NAMES,
                TOPIC_PARTITIONS,
                mkSet("store1"),
                STANDBY_TOPIC_PARTITIONS);
        assertNotEquals(streamsMetadata, differStandByStores);
        assertNotEquals(streamsMetadata.hashCode(), differStandByStores.hashCode());

        final StreamsMetadata differStandByTopicPartitions = new StreamsMetadataImpl(
                HOST_INFO,
                STATE_STORE_NAMES,
                TOPIC_PARTITIONS,
                STAND_BY_STORE_NAMES,
                mkSet(TP_0));
        assertNotEquals(streamsMetadata, differStandByTopicPartitions);
        assertNotEquals(streamsMetadata.hashCode(), differStandByTopicPartitions.hashCode());
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
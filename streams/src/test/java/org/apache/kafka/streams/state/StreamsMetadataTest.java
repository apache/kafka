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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class StreamsMetadataTest {

    private static final HostInfo HOST_INFO = new HostInfo("local", 12);
    public static final Set<String> STATE_STORE_NAMES = mkSet("store1", "store2");
    private static final TopicPartition TP_0 = new TopicPartition("t", 0);
    private static final TopicPartition TP_1 = new TopicPartition("t", 1);
    public static final Set<TopicPartition> TOPIC_PARTITIONS = mkSet(TP_0, TP_1);
    public static final Set<String> STAND_BY_STORE_NAMES = mkSet("store2");
    public static final Set<TopicPartition> STANDBY_TOPIC_PARTITIONS = mkSet(TP_1);

    private StreamsMetadata streamsMetadata;

    @BeforeEach
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
        assertThat(isUnmodifiable(streamsMetadata.stateStoreNames()), is(true));
        assertThat(isUnmodifiable(streamsMetadata.topicPartitions()), is(true));
        assertThat(isUnmodifiable(streamsMetadata.standbyTopicPartitions()), is(true));
        assertThat(isUnmodifiable(streamsMetadata.standbyStateStoreNames()), is(true));
    }

    @Test
    public void shouldBeEqualsIfSameObject() {
        final StreamsMetadata same = new StreamsMetadataImpl(
            HOST_INFO,
            STATE_STORE_NAMES,
            TOPIC_PARTITIONS,
            STAND_BY_STORE_NAMES,
            STANDBY_TOPIC_PARTITIONS);
        assertThat(streamsMetadata, equalTo(same));
        assertThat(streamsMetadata.hashCode(), equalTo(same.hashCode()));
    }

    @Test
    public void shouldNotBeEqualIfDifferInHostInfo() {
        final StreamsMetadata differHostInfo = new StreamsMetadataImpl(
            new HostInfo("different", 122),
            STATE_STORE_NAMES,
            TOPIC_PARTITIONS,
            STAND_BY_STORE_NAMES,
            STANDBY_TOPIC_PARTITIONS);
        assertThat(streamsMetadata, not(equalTo(differHostInfo)));
        assertThat(streamsMetadata.hashCode(), not(equalTo(differHostInfo.hashCode())));
    }

    @Test
    public void shouldNotBeEqualIfDifferStateStoreNames() {
        final StreamsMetadata differStateStoreNames = new StreamsMetadataImpl(
            HOST_INFO,
            mkSet("store1"),
            TOPIC_PARTITIONS,
            STAND_BY_STORE_NAMES,
            STANDBY_TOPIC_PARTITIONS);
        assertThat(streamsMetadata, not(equalTo(differStateStoreNames)));
        assertThat(streamsMetadata.hashCode(), not(equalTo(differStateStoreNames.hashCode())));
    }

    @Test
    public void shouldNotBeEqualIfDifferInTopicPartitions() {
        final StreamsMetadata differTopicPartitions = new StreamsMetadataImpl(
            HOST_INFO,
            STATE_STORE_NAMES,
            mkSet(TP_0),
            STAND_BY_STORE_NAMES,
            STANDBY_TOPIC_PARTITIONS);
        assertThat(streamsMetadata, not(equalTo(differTopicPartitions)));
        assertThat(streamsMetadata.hashCode(), not(equalTo(differTopicPartitions.hashCode())));
    }

    @Test
    public void shouldNotBeEqualIfDifferInStandByStores() {
        final StreamsMetadata differStandByStores = new StreamsMetadataImpl(
            HOST_INFO,
            STATE_STORE_NAMES,
            TOPIC_PARTITIONS,
            mkSet("store1"),
            STANDBY_TOPIC_PARTITIONS);
        assertThat(streamsMetadata, not(equalTo(differStandByStores)));
        assertThat(streamsMetadata.hashCode(), not(equalTo(differStandByStores.hashCode())));
    }

    @Test
    public void shouldNotBeEqualIfDifferInStandByTopicPartitions() {
        final StreamsMetadata differStandByTopicPartitions = new StreamsMetadataImpl(
            HOST_INFO,
            STATE_STORE_NAMES,
            TOPIC_PARTITIONS,
            STAND_BY_STORE_NAMES,
            mkSet(TP_0));
        assertThat(streamsMetadata, not(equalTo(differStandByTopicPartitions)));
        assertThat(streamsMetadata.hashCode(), not(equalTo(differStandByTopicPartitions.hashCode())));
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
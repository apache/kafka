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
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.processor.TaskId;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ThreadMetadataImplTest {

    public static final String THREAD_NAME = "thread name";
    public static final String THREAD_STATE = "thread state";
    public static final String MAIN_CONSUMER_CLIENT_ID = "main Consumer ClientID";
    public static final String RESTORE_CONSUMER_CLIENT_ID = "restore Consumer ClientID";
    public static final String CLIENT_ID_1 = "client Id 1";
    public static final String PRODUCER_CLIENT_IDS = CLIENT_ID_1;
    public static final TaskId TASK_ID_0 = new TaskId(1, 2);
    public static final TaskId TASK_ID_1 = new TaskId(1, 1);
    public static final TopicPartition TP_0_0 = new TopicPartition("t", 0);
    public static final TopicPartition TP_1_0 = new TopicPartition("t", 1);
    public static final TopicPartition TP_0_1 = new TopicPartition("t", 2);
    public static final TopicPartition TP_1_1 = new TopicPartition("t", 3);
    public static final TaskMetadata TM_0 = new TaskMetadataImpl(
        TASK_ID_0,
        Set.of(TP_0_0, TP_1_0),
        mkMap(mkEntry(TP_0_0, 1L), mkEntry(TP_1_0, 2L)),
        mkMap(mkEntry(TP_0_0, 1L), mkEntry(TP_1_0, 2L)),
        Optional.of(3L));
    public static final TaskMetadata TM_1 = new TaskMetadataImpl(
        TASK_ID_1,
        Set.of(TP_0_1, TP_1_1),
        mkMap(mkEntry(TP_0_1, 1L), mkEntry(TP_1_1, 2L)),
        mkMap(mkEntry(TP_0_1, 1L), mkEntry(TP_1_1, 2L)),
        Optional.of(3L));
    public static final Set<TaskMetadata> STANDBY_TASKS = Set.of(TM_0, TM_1);
    public static final Set<TaskMetadata> ACTIVE_TASKS = Set.of(TM_1);
    public static final String ADMIN_CLIENT_ID = "admin ClientID";

    private ThreadMetadata threadMetadata;

    @BeforeEach
    public void setUp() {
        threadMetadata = new ThreadMetadataImpl(
            THREAD_NAME,
            THREAD_STATE,
            MAIN_CONSUMER_CLIENT_ID,
            RESTORE_CONSUMER_CLIENT_ID,
            PRODUCER_CLIENT_IDS,
            ADMIN_CLIENT_ID,
            ACTIVE_TASKS,
            STANDBY_TASKS
        );
    }

    @Test
    public void shouldNotAllowModificationOfInternalStateViaGetters() {
        assertThat(isUnmodifiable(threadMetadata.producerClientIds()), is(true));
        assertThat(isUnmodifiable(threadMetadata.activeTasks()), is(true));
        assertThat(isUnmodifiable(threadMetadata.standbyTasks()), is(true));
    }

    @Test
    public void shouldBeEqualIfSameObject() {
        final ThreadMetadata same = new ThreadMetadataImpl(
            THREAD_NAME,
            THREAD_STATE,
            MAIN_CONSUMER_CLIENT_ID,
            RESTORE_CONSUMER_CLIENT_ID,
            PRODUCER_CLIENT_IDS,
            ADMIN_CLIENT_ID,
            ACTIVE_TASKS,
            STANDBY_TASKS
        );
        assertThat(threadMetadata, equalTo(same));
        assertThat(threadMetadata.hashCode(), equalTo(same.hashCode()));
    }

    @Test
    public void shouldNotBeEqualIfDifferInThreadName() {
        final ThreadMetadata differThreadName = new ThreadMetadataImpl(
            "different",
            THREAD_STATE,
            MAIN_CONSUMER_CLIENT_ID,
            RESTORE_CONSUMER_CLIENT_ID,
            PRODUCER_CLIENT_IDS,
            ADMIN_CLIENT_ID,
            ACTIVE_TASKS,
            STANDBY_TASKS
        );
        assertThat(threadMetadata, not(equalTo(differThreadName)));
        assertThat(threadMetadata.hashCode(), not(equalTo(differThreadName.hashCode())));
    }

    @Test
    public void shouldNotBeEqualIfDifferInThreadState() {
        final ThreadMetadata differThreadState = new ThreadMetadataImpl(
            THREAD_NAME,
            "different",
            MAIN_CONSUMER_CLIENT_ID,
            RESTORE_CONSUMER_CLIENT_ID,
            PRODUCER_CLIENT_IDS,
            ADMIN_CLIENT_ID,
            ACTIVE_TASKS,
            STANDBY_TASKS
        );
        assertThat(threadMetadata, not(equalTo(differThreadState)));
        assertThat(threadMetadata.hashCode(), not(equalTo(differThreadState.hashCode())));
    }

    @Test
    public void shouldNotBeEqualIfDifferInClientId() {
        final ThreadMetadata differMainConsumerClientId = new ThreadMetadataImpl(
            THREAD_NAME,
            THREAD_STATE,
            "different",
            RESTORE_CONSUMER_CLIENT_ID,
            PRODUCER_CLIENT_IDS,
            ADMIN_CLIENT_ID,
            ACTIVE_TASKS,
            STANDBY_TASKS
        );
        assertThat(threadMetadata, not(equalTo(differMainConsumerClientId)));
        assertThat(threadMetadata.hashCode(), not(equalTo(differMainConsumerClientId.hashCode())));
    }

    @Test
    public void shouldNotBeEqualIfDifferInConsumerClientId() {
        final ThreadMetadata differRestoreConsumerClientId = new ThreadMetadataImpl(
            THREAD_NAME,
            THREAD_STATE,
            MAIN_CONSUMER_CLIENT_ID,
            "different",
            PRODUCER_CLIENT_IDS,
            ADMIN_CLIENT_ID,
            ACTIVE_TASKS,
            STANDBY_TASKS
        );
        assertThat(threadMetadata, not(equalTo(differRestoreConsumerClientId)));
        assertThat(threadMetadata.hashCode(), not(equalTo(differRestoreConsumerClientId.hashCode())));
    }

    @Test
    public void shouldNotBeEqualIfDifferInProducerClientIds() {
        final ThreadMetadata differProducerClientIds = new ThreadMetadataImpl(
            THREAD_NAME,
            THREAD_STATE,
            MAIN_CONSUMER_CLIENT_ID,
            RESTORE_CONSUMER_CLIENT_ID,
            "different-producer-client-id",
            ADMIN_CLIENT_ID,
            ACTIVE_TASKS,
            STANDBY_TASKS
        );
        assertThat(threadMetadata, not(equalTo(differProducerClientIds)));
        assertThat(threadMetadata.hashCode(), not(equalTo(differProducerClientIds.hashCode())));
    }

    @Test
    public void shouldNotBeEqualIfDifferInAdminClientId() {
        final ThreadMetadata differAdminClientId = new ThreadMetadataImpl(
            THREAD_NAME,
            THREAD_STATE,
            MAIN_CONSUMER_CLIENT_ID,
            RESTORE_CONSUMER_CLIENT_ID,
            PRODUCER_CLIENT_IDS,
            "different",
            ACTIVE_TASKS,
            STANDBY_TASKS
        );
        assertThat(threadMetadata, not(equalTo(differAdminClientId)));
        assertThat(threadMetadata.hashCode(), not(equalTo(differAdminClientId.hashCode())));
    }

    @Test
    public void shouldNotBeEqualIfDifferInActiveTasks() {
        final ThreadMetadata differActiveTasks = new ThreadMetadataImpl(
            THREAD_NAME,
            THREAD_STATE,
            MAIN_CONSUMER_CLIENT_ID,
            RESTORE_CONSUMER_CLIENT_ID,
            PRODUCER_CLIENT_IDS,
            ADMIN_CLIENT_ID,
            Set.of(TM_0),
            STANDBY_TASKS
        );
        assertThat(threadMetadata, not(equalTo(differActiveTasks)));
        assertThat(threadMetadata.hashCode(), not(equalTo(differActiveTasks.hashCode())));
    }

    @Test
    public void shouldNotBeEqualIfDifferInStandByTasks() {
        final ThreadMetadata differStandByTasks = new ThreadMetadataImpl(
            THREAD_NAME,
            THREAD_STATE,
            MAIN_CONSUMER_CLIENT_ID,
            RESTORE_CONSUMER_CLIENT_ID,
            PRODUCER_CLIENT_IDS,
            ADMIN_CLIENT_ID,
            ACTIVE_TASKS,
            Set.of(TM_0)
        );
        assertThat(threadMetadata, not(equalTo(differStandByTasks)));
        assertThat(threadMetadata.hashCode(), not(equalTo(differStandByTasks.hashCode())));
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

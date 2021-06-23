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
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ThreadMetadataImplTest {

    public static final String THREAD_NAME = "thread name";
    public static final String THREAD_STATE = "thread state";
    public static final String MAIN_CONSUMER_CLIENT_ID = "main Consumer ClientID";
    public static final String RESTORE_CONSUMER_CLIENT_ID = "restore Consumer ClientID";
    public static final String CLIENT_ID_1 = "client Id 1";
    public static final String CLIENT_ID_2 = "client Id 2";
    public static final Set<String> PRODUCER_CLIENT_IDS = mkSet(CLIENT_ID_1, CLIENT_ID_2);
    public static final TaskId TASK_ID_0 = new TaskId(1, 2);
    public static final TaskId TASK_ID_1 = new TaskId(1, 1);
    public static final TopicPartition TP_0_0 = new TopicPartition("t", 0);
    public static final TopicPartition TP_1_0 = new TopicPartition("t", 1);
    public static final TopicPartition TP_0_1 = new TopicPartition("t", 2);
    public static final TopicPartition TP_1_1 = new TopicPartition("t", 3);
    public static final TaskMetadata TM_0 = new TaskMetadataImpl(
            TASK_ID_0,
            mkSet(TP_0_0, TP_1_0),
            mkMap(mkEntry(TP_0_0, 1L), mkEntry(TP_1_0, 2L)),
            mkMap(mkEntry(TP_0_0, 1L), mkEntry(TP_1_0, 2L)),
            Optional.of(3L));
    public static final TaskMetadata TM_1 = new TaskMetadataImpl(
            TASK_ID_1,
            mkSet(TP_0_1, TP_1_1),
            mkMap(mkEntry(TP_0_1, 1L), mkEntry(TP_1_1, 2L)),
            mkMap(mkEntry(TP_0_1, 1L), mkEntry(TP_1_1, 2L)),
            Optional.of(3L));
    public static final Set<TaskMetadata> STANDBY_TASKS = mkSet(TM_0, TM_1);
    public static final Set<TaskMetadata> ACTIVE_TASKS = mkSet(TM_1);
    public static final String ADMIN_CLIENT_ID = "admin ClientID";

    private ThreadMetadata threadMetadata;

    @Before
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
        assertTrue(isUnmodifiable(threadMetadata.producerClientIds()));
        assertTrue(isUnmodifiable(threadMetadata.activeTasks()));
        assertTrue(isUnmodifiable(threadMetadata.standbyTasks()));
    }

    @Test
    public void shouldFollowHashCodeAndEqualsContract() {
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
        assertEquals(threadMetadata, same);
        assertEquals(threadMetadata.hashCode(), same.hashCode());

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
        assertNotEquals(threadMetadata, differThreadName);
        assertNotEquals(threadMetadata.hashCode(), differThreadName.hashCode());

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
        assertNotEquals(threadMetadata, differThreadState);
        assertNotEquals(threadMetadata.hashCode(), differThreadState.hashCode());

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
        assertNotEquals(threadMetadata, differMainConsumerClientId);
        assertNotEquals(threadMetadata.hashCode(), differMainConsumerClientId.hashCode());

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
        assertNotEquals(threadMetadata, differRestoreConsumerClientId);
        assertNotEquals(threadMetadata.hashCode(), differRestoreConsumerClientId.hashCode());


        final ThreadMetadata differProducerClientIds = new ThreadMetadataImpl(
                THREAD_NAME,
                THREAD_STATE,
                MAIN_CONSUMER_CLIENT_ID,
                RESTORE_CONSUMER_CLIENT_ID,
                mkSet(CLIENT_ID_1),
                ADMIN_CLIENT_ID,
                ACTIVE_TASKS,
                STANDBY_TASKS
        );
        assertNotEquals(threadMetadata, differProducerClientIds);
        assertNotEquals(threadMetadata.hashCode(), differProducerClientIds.hashCode());

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
        assertNotEquals(threadMetadata, differAdminClientId);
        assertNotEquals(threadMetadata.hashCode(), differAdminClientId.hashCode());


        final ThreadMetadata differActiveTasks = new ThreadMetadataImpl(
                THREAD_NAME,
                THREAD_STATE,
                MAIN_CONSUMER_CLIENT_ID,
                RESTORE_CONSUMER_CLIENT_ID,
                PRODUCER_CLIENT_IDS,
                ADMIN_CLIENT_ID,
                mkSet(TM_0),
                STANDBY_TASKS
        );
        assertNotEquals(threadMetadata, differActiveTasks);
        assertNotEquals(threadMetadata.hashCode(), differActiveTasks.hashCode());

        final ThreadMetadata differStandByTasks = new ThreadMetadataImpl(
                THREAD_NAME,
                THREAD_STATE,
                MAIN_CONSUMER_CLIENT_ID,
                RESTORE_CONSUMER_CLIENT_ID,
                PRODUCER_CLIENT_IDS,
                ADMIN_CLIENT_ID,
                ACTIVE_TASKS,
                mkSet(TM_0)
                );
        assertNotEquals(threadMetadata, differStandByTasks);
        assertNotEquals(threadMetadata.hashCode(), differStandByTasks.hashCode());
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

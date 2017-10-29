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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.test.MockRestoreCallback;
import org.apache.kafka.test.MockStateRestoreListener;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class StateRestorerTest {

    private static final long OFFSET_LIMIT = 50;
    private final MockRestoreCallback callback = new MockRestoreCallback();
    private final MockStateRestoreListener reportingListener = new MockStateRestoreListener();
    private final CompositeRestoreListener compositeRestoreListener = new CompositeRestoreListener(callback);
    private final StateRestorer restorer = new StateRestorer(new TopicPartition("topic", 1), compositeRestoreListener,
                                                             null, OFFSET_LIMIT, true, "storeName");

    @Before
    public void setUp() {
        compositeRestoreListener.setUserRestoreListener(reportingListener);
    }

    @Test
    public void shouldCallRestoreOnRestoreCallback() {
        restorer.restore(Collections.singletonList(KeyValue.pair(new byte[0], new byte[0])));
        assertThat(callback.restored.size(), equalTo(1));
    }

    @Test
    public void shouldBeCompletedIfRecordOffsetGreaterThanEndOffset() {
        assertTrue(restorer.hasCompleted(11, 10));
    }

    @Test
    public void shouldBeCompletedIfRecordOffsetGreaterThanOffsetLimit() {
        assertTrue(restorer.hasCompleted(51, 100));
    }

    @Test
    public void shouldBeCompletedIfEndOffsetAndRecordOffsetAreZero() {
        assertTrue(restorer.hasCompleted(0, 0));
    }

    @Test
    public void shouldBeCompletedIfOffsetAndOffsetLimitAreZero() {
        final StateRestorer
            restorer =
            new StateRestorer(new TopicPartition("topic", 1), compositeRestoreListener, null, 0, true,
                              "storeName");
        assertTrue(restorer.hasCompleted(0, 10));
    }

    @Test
    public void shouldSetRestoredOffsetToMinOfLimitAndOffset() {
        restorer.setRestoredOffset(20);
        assertThat(restorer.restoredOffset(), equalTo(20L));
        restorer.setRestoredOffset(100);
        assertThat(restorer.restoredOffset(), equalTo(OFFSET_LIMIT));
    }

    @Test
    public void shouldSetStartingOffsetToMinOfLimitAndOffset() {
        restorer.setStartingOffset(20);
        assertThat(restorer.startingOffset(), equalTo(20L));
        restorer.setRestoredOffset(100);
        assertThat(restorer.restoredOffset(), equalTo(OFFSET_LIMIT));
    }

    @Test
    public void shouldReturnCorrectNumRestoredRecords() {
        restorer.setStartingOffset(20);
        restorer.setRestoredOffset(40);
        assertThat(restorer.restoredNumRecords(), equalTo(20L));
        restorer.setRestoredOffset(100);
        assertThat(restorer.restoredNumRecords(), equalTo(OFFSET_LIMIT - 20));
    }
}
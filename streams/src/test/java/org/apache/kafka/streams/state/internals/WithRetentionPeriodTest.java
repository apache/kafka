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

import org.apache.kafka.streams.processor.StateStore;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class WithRetentionPeriodTest {

    private static final long RETENTION_MS = 60_000L;

    private static class TestWrapper extends WrappedStateStore<StateStore, Object, Object> {
        TestWrapper(final StateStore wrapped) {
            super(wrapped);
        }
    }

    private static StateStore storeReportingRetention() {
        final StateStore store = mock(StateStore.class, withSettings().extraInterfaces(WithRetentionPeriod.class));
        when(((WithRetentionPeriod) store).retentionPeriod()).thenReturn(RETENTION_MS);
        return store;
    }

    @Test
    public void shouldReturnRetentionOfStoreThatReportsItDirectly() {
        assertEquals(RETENTION_MS, WithRetentionPeriod.resolveRetentionPeriod(storeReportingRetention()));
    }

    @Test
    public void shouldResolveRetentionThroughWrappedStores() {
        final StateStore wrapped = new TestWrapper(new TestWrapper(storeReportingRetention()));

        assertEquals(RETENTION_MS, WithRetentionPeriod.resolveRetentionPeriod(wrapped));
    }

    @Test
    public void shouldReturnMinusOneWhenStoreDoesNotReportRetention() {
        assertEquals(-1L, WithRetentionPeriod.resolveRetentionPeriod(mock(StateStore.class)));
    }

    @Test
    public void shouldReturnMinusOneWhenNoWrappedStoreReportsRetention() {
        final StateStore wrapped = new TestWrapper(new TestWrapper(mock(StateStore.class)));

        assertEquals(-1L, WithRetentionPeriod.resolveRetentionPeriod(wrapped));
    }

    @Test
    public void shouldNotConsultOuterLayersThatReportRetention() {
        // retention belongs to the innermost store, so an outer layer reporting one is ignored
        final StateStore outer = new RetentionReportingWrapper(mock(StateStore.class));

        assertEquals(-1L, WithRetentionPeriod.resolveRetentionPeriod(outer));
    }

    private static class RetentionReportingWrapper extends WrappedStateStore<StateStore, Object, Object>
        implements WithRetentionPeriod {

        RetentionReportingWrapper(final StateStore wrapped) {
            super(wrapped);
        }

        @Override
        public long retentionPeriod() {
            return RETENTION_MS;
        }
    }
}

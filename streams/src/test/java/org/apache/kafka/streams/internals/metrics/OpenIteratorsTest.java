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
package org.apache.kafka.streams.internals.metrics;

import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.MeteredIterator;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
public class OpenIteratorsTest {

    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);
    final ArgumentCaptor<Gauge<Long>> gaugeCaptor = ArgumentCaptor.forClass(Gauge.class);
    final OpenIterators openIterators = new OpenIterators();

    @BeforeEach
    public void setUp() {
        StateStoreMetrics.addOldestOpenIteratorGauge("taskId", "metricsScope", "name", streamsMetrics,
            (config, now) -> openIterators.oldestStartTimestamp());
        verify(streamsMetrics).addStoreLevelMutableMetric(any(), any(), any(), any(), any(), any(), gaugeCaptor.capture());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCalculateOldestStartTimestampCorrectly() {
        final Gauge<Long> gauge = gaugeCaptor.getValue();

        final MeteredIterator meteredIterator1 = () -> 5;
        final MeteredIterator meteredIterator2 = () -> 2;
        final MeteredIterator meteredIterator3 = () -> 6;

        openIterators.add(meteredIterator1);
        assertThat(gauge.value(null, 0), is(5L));

        openIterators.add(meteredIterator2);
        assertThat(gauge.value(null, 0), is(2L));

        openIterators.remove(meteredIterator2);
        assertThat(gauge.value(null, 0), is(5L));

        openIterators.remove(meteredIterator1);
        assertThat(gauge.value(null, 0), is(0L));

        openIterators.add(meteredIterator3);
        assertThat(gauge.value(null, 0), is(6L));
    }
}

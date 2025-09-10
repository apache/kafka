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
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.MeteredIterator;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

public class OpenIteratorsTest {

    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCalculateOldestStartTimestampCorrectly() {
        final OpenIterators openIterators = new OpenIterators(new TaskId(0, 0), "scope", "name", streamsMetrics);

        final MeteredIterator meteredIterator1 = () -> 5;
        final MeteredIterator meteredIterator2 = () -> 2;
        final MeteredIterator meteredIterator3 = () -> 6;

        openIterators.add(meteredIterator1);
        final ArgumentCaptor<Gauge<Long>> gaugeCaptor = ArgumentCaptor.forClass(Gauge.class);
        verify(streamsMetrics).addStoreLevelMutableMetric(any(), any(), any(), any(), any(), any(), gaugeCaptor.capture());
        final Gauge<Long> gauge = gaugeCaptor.getValue();
        assertThat(gauge.value(null, 0), is(5L));
        reset(streamsMetrics);

        openIterators.add(meteredIterator2);
        verify(streamsMetrics, never()).addStoreLevelMutableMetric(any(), any(), any(), any(), any(), any(), gaugeCaptor.capture());
        assertThat(gauge.value(null, 0), is(2L));

        openIterators.remove(meteredIterator2);
        verify(streamsMetrics, never()).removeMetric(any());
        assertThat(gauge.value(null, 0), is(5L));

        openIterators.remove(meteredIterator1);
        verify(streamsMetrics).removeMetric(any());
        assertThat(gauge.value(null, 0), is(5L));

        openIterators.add(meteredIterator3);
        verify(streamsMetrics).addStoreLevelMutableMetric(any(), any(), any(), any(), any(), any(), gaugeCaptor.capture());
        assertThat(gaugeCaptor.getValue(), not(gauge));
        assertThat(gaugeCaptor.getValue().value(null, 0), is(6L));
    }
}

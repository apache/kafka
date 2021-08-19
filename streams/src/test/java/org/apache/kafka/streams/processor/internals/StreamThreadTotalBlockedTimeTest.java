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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class StreamThreadTotalBlockedTimeTest {
    @Mock
    Consumer<?, ?> consumer;
    @Mock
    Consumer<?, ?> restoreConsumer;
    @Mock
    Supplier<Double> producerBlocked;

    private StreamThreadTotalBlockedTime blockedTime;

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Before
    public void setup() {
        blockedTime = new StreamThreadTotalBlockedTime(consumer, restoreConsumer, producerBlocked);
        when(consumer.metrics()).thenAnswer(a -> new MetricsBuilder()
            .addMetric("iotime-total", 1)
            .addMetric("io-waittime-total", 2)
            .addMetric("committed-time-total", 3)
            .addMetric("commit-sync-time-total", 4)
            .build()
        );
        when(restoreConsumer.metrics()).thenAnswer(a -> new MetricsBuilder()
            .addMetric("iotime-total", 5)
            .addMetric("io-waittime-total", 6)
            .build()
        );
        when(producerBlocked.get()).thenReturn(7.0);
    }

    @Test
    public void shouldComputeTotalBlockedTime() {
        assertThat(
            blockedTime.compute(),
            equalTo(1.0 + 2.0 + 3.0 + 4.0 + 5.0 + 6.0 + 7.0)
        );
    }

    private static class MetricsBuilder {
        private final HashMap<MetricName, Metric> metrics = new HashMap<>();

        private MetricsBuilder addMetric(final String name, final double value) {
            final MetricName metricName = new MetricName(name, "", "", Collections.emptyMap());
            metrics.put(
                metricName,
                new Metric() {
                    @Override
                    public MetricName metricName() {
                        return metricName;
                    }

                    @Override
                    public Object metricValue() {
                        return value;
                    }
                }
            );
            return this;
        }

        public Map<MetricName, ? extends Metric> build() {
            return Collections.unmodifiableMap(metrics);
        }
    }
}
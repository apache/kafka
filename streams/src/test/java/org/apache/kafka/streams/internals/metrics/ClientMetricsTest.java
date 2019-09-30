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
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.junit.Test;

import static org.easymock.EasyMock.and;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.not;
import static org.easymock.EasyMock.notNull;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;

public class ClientMetricsTest {

    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);

    private interface OneParamMetricAdder {
        void addMetric(final StreamsMetricsImpl streamsMetrics);
    }

    private interface TwoParamMetricAdder {
        void addMetric(final StreamsMetricsImpl streamsMetrics, final String value);
    }

    /*
     * This test may fail when executed from a IDE since it expects the /kafka/kafka-streams-version.properties on the
     * class path.
     */
    @Test
    public void shouldAddVersionMetric() {
        final String name = "version";
        final String description = "The version of the Kafka Streams client";
        setUpAndVerifyMetricOneParam(name, description, ClientMetrics::addVersionMetric);
    }

    /*
     * This test may fail when executed from a IDE since it expects the /kafka/kafka-streams-version.properties on the
     * class path.
     */
    @Test
    public void shouldAddCommitIdMetric() {
        final String name = "commit-id";
        final String description = "The version control commit ID of the Kafka Streams client";
        setUpAndVerifyMetricOneParam(name, description, ClientMetrics::addCommitIdMetric);
    }

    @Test
    public void shouldAddApplicationIdMetric() {
        final String name = "application-id";
        final String description = "The application ID of the Kafka Streams client";
        setUpAndVerifyMetricTwoParam(name, description, "thisIsAnID", ClientMetrics::addApplicationIdMetric);
    }

    @Test
    public void shouldAddTopologyDescriptionMetric() {
        final String name = "topology-description";
        final String description = "The description of the topology executed in the Kafka Streams client";
        setUpAndVerifyMetricTwoParam(
            name,
            description,
            "thisIsATopologyDescription",
            ClientMetrics::addTopologyDescriptionMetric
        );
    }

    @Test
    public void shouldAddStateMetric() {
        final String name = "state";
        final String description = "The state of the Kafka Streams client";
        final Gauge<State> stateProvider = (config, now) -> State.RUNNING;
        streamsMetrics.addClientLevelMutableMetric(
            eq(name),
            eq(description),
            eq(RecordingLevel.INFO),
            eq(stateProvider)
        );
        replay(streamsMetrics);

        ClientMetrics.addStateMetric(streamsMetrics, stateProvider);

        verify(streamsMetrics);
    }

    private void setUpAndVerifyMetricOneParam(final String name,
                                              final String description,
                                              final OneParamMetricAdder metricAdder) {
        streamsMetrics.addClientLevelImmutableMetric(
            eq(name),
            eq(description),
            eq(RecordingLevel.INFO),
            and(not(eq("unknown")), notNull())
        );
        replay(streamsMetrics);

        metricAdder.addMetric(streamsMetrics);

        verify(streamsMetrics);
    }

    private void setUpAndVerifyMetricTwoParam(final String name,
                                              final String description,
                                              final String value,
                                              final TwoParamMetricAdder metricAdder) {
        streamsMetrics.addClientLevelImmutableMetric(
            eq(name),
            eq(description),
            eq(RecordingLevel.INFO),
            eq(value)
        );
        replay(streamsMetrics);

        metricAdder.addMetric(streamsMetrics, value);

        verify(streamsMetrics);
    }
}
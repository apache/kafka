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
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.CLIENT_LEVEL_GROUP;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;

@RunWith(PowerMockRunner.class)
@PrepareForTest({StreamsMetricsImpl.class, Sensor.class, ClientMetrics.class})
public class ClientMetricsTest {
    private static final String COMMIT_ID = "test-commit-ID";
    private static final String VERSION = "test-version";

    private final StreamsMetricsImpl streamsMetrics = createMock(StreamsMetricsImpl.class);
    private final Sensor expectedSensor = createMock(Sensor.class);
    private final Map<String, String> tagMap = Collections.singletonMap("hello", "world");

    @Before
    public void setUp() {
        mockStatic(StreamsMetricsImpl.class);
    }

    @Test
    public void shouldAddVersionMetric() {
        final String name = "version";
        final String description = "The version of the Kafka Streams client";
        setUpAndVerifyImmutableMetric(name, description, VERSION, () -> ClientMetrics.addVersionMetric(streamsMetrics));
    }

    @Test
    public void shouldAddCommitIdMetric() {
        final String name = "commit-id";
        final String description = "The version control commit ID of the Kafka Streams client";
        setUpAndVerifyImmutableMetric(name, description, COMMIT_ID, () -> ClientMetrics.addCommitIdMetric(streamsMetrics));
    }

    @Test
    public void shouldAddApplicationIdMetric() {
        final String name = "application-id";
        final String description = "The application ID of the Kafka Streams client";
        final String applicationId = "thisIsAnID";
        setUpAndVerifyImmutableMetric(
            name,
            description,
            applicationId,
            () -> ClientMetrics.addApplicationIdMetric(streamsMetrics, applicationId)
        );
    }

    @Test
    public void shouldAddTopologyDescriptionMetric() {
        final String name = "topology-description";
        final String description = "The description of the topology executed in the Kafka Streams client";
        final String topologyDescription = "thisIsATopologyDescription";
        setUpAndVerifyImmutableMetric(
            name,
            description,
            topologyDescription,
            () -> ClientMetrics.addTopologyDescriptionMetric(streamsMetrics, topologyDescription)
        );
    }

    @Test
    public void shouldAddStateMetric() {
        final String name = "state";
        final String description = "The state of the Kafka Streams client";
        final Gauge<State> stateProvider = (config, now) -> State.RUNNING;
        setUpAndVerifyMutableMetric(
            name,
            description,
            stateProvider,
            () -> ClientMetrics.addStateMetric(streamsMetrics, stateProvider)
        );
    }

    @Test
    public void shouldAddAliveStreamThreadsMetric() {
        final String name = "alive-stream-threads";
        final String description = "The current number of alive stream threads that are running or participating in rebalance";
        final Gauge<Integer> valueProvider = (config, now) -> 1;
        setUpAndVerifyMutableMetric(
            name,
            description,
            valueProvider,
            () -> ClientMetrics.addNumAliveStreamThreadMetric(streamsMetrics, valueProvider)
        );
    }

    @Test
    public void shouldGetFailedStreamThreadsSensor() {
        final String name = "failed-stream-threads";
        final String description = "The number of failed stream threads since the start of the Kafka Streams client";
        expect(streamsMetrics.clientLevelSensor(name, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.clientLevelTagMap()).andReturn(tagMap);
        StreamsMetricsImpl.addSumMetricToSensor(
            expectedSensor,
            CLIENT_LEVEL_GROUP,
            tagMap,
            name,
            false,
            description
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = ClientMetrics.failedStreamThreadSensor(streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    private <K> void setUpAndVerifyMutableMetric(final String name,
                                                 final String description,
                                                 final Gauge<K> valueProvider,
                                                 final Runnable metricAdder) {
        streamsMetrics.addClientLevelMutableMetric(
            eq(name),
            eq(description),
            eq(RecordingLevel.INFO),
            eq(valueProvider)
        );
        replay(streamsMetrics);

        metricAdder.run();

        verify(streamsMetrics);
    }

    private void setUpAndVerifyImmutableMetric(final String name,
                                               final String description,
                                               final String value,
                                               final Runnable metricAdder) {
        streamsMetrics.addClientLevelImmutableMetric(
            eq(name),
            eq(description),
            eq(RecordingLevel.INFO),
            eq(value)
        );
        replay(streamsMetrics);

        metricAdder.run();

        verify(streamsMetrics);
    }
}
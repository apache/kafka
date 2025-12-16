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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.controller.metrics.ControllerMetadataMetrics;

import com.yammer.metrics.core.MetricsRegistry;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultExternalKRaftMetricsTest {

    @Test
    public void testDefaultExternalKRaftMetrics() {
        BrokerServerMetrics brokerServerMetrics = new BrokerServerMetrics(new Metrics());
        ControllerMetadataMetrics controllerMetadataMetrics = new ControllerMetadataMetrics(Optional.of(new MetricsRegistry()));
        DefaultExternalKRaftMetrics metrics = new DefaultExternalKRaftMetrics(
                Optional.of(brokerServerMetrics),
                Optional.of(controllerMetadataMetrics)
        );

        assertFalse(brokerServerMetrics.ignoredStaticVoters());
        assertFalse(controllerMetadataMetrics.ignoredStaticVoters());

        metrics.setIgnoredStaticVoters(true);

        assertTrue(brokerServerMetrics.ignoredStaticVoters());
        assertTrue(controllerMetadataMetrics.ignoredStaticVoters());

        metrics.setIgnoredStaticVoters(false);

        assertFalse(brokerServerMetrics.ignoredStaticVoters());
        assertFalse(controllerMetadataMetrics.ignoredStaticVoters());
    }

    @Test
    public void testEmptyDefaultExternalKRaftMetrics() {
        DefaultExternalKRaftMetrics metrics = new DefaultExternalKRaftMetrics(Optional.empty(), Optional.empty());
        metrics.setIgnoredStaticVoters(true);
    }
}

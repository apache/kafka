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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetricsVerbosityControllerTest {

    private static AbstractConfig configWith(String raw) {
        ConfigDef def = new ConfigDef()
            .define(MetricConfigs.METRICS_VERBOSITY_CONFIG,
                ConfigDef.Type.STRING,
                MetricConfigs.METRICS_VERBOSITY_DEFAULT,
                ConfigDef.Importance.LOW,
                MetricConfigs.METRICS_VERBOSITY_DOC);
        return new AbstractConfig(def, Map.of(MetricConfigs.METRICS_VERBOSITY_CONFIG, raw));
    }

    @BeforeEach
    public void resetCache() {
        // Ensure a known baseline for the static cache between tests
        AbstractConfig reset = configWith("[]");
        MetricsVerbosityController.shouldEmitPartitionMetric(reset, "AnyMetric", "any-topic");
    }

    @Test
    public void testEmptyConfigEmitsFalse() {
        AbstractConfig conf = configWith("[]");
        assertFalse(MetricsVerbosityController.shouldEmitPartitionMetric(conf, "BytesInPerSec", "car"));
    }

    @Test
    public void testInvalidJsonEmitsFalse() {
        AbstractConfig conf = configWith("not json");
        assertFalse(MetricsVerbosityController.shouldEmitPartitionMetric(conf, "BytesInPerSec", "car"));
    }

    @Test
    public void testLowLevelIgnored() {
        String raw = "[ { \"level\": \"low\", \"names\": \"Bytes.*\", \"filters\": [{\"topics\":[\"car\"]}] } ]";
        AbstractConfig conf = configWith(raw);
        assertFalse(MetricsVerbosityController.shouldEmitPartitionMetric(conf, "BytesInPerSec", "car"));
    }

    @Test
    public void testHighLevelNoTopicsDoesNotMatch() {
        String raw = "[ { \"level\": \"high\", \"names\": \"Bytes.*\" } ]";
        AbstractConfig conf = configWith(raw);
        assertFalse(MetricsVerbosityController.shouldEmitPartitionMetric(conf, "BytesInPerSec", "car"));
    }

    @Test
    public void testHighLevelWithTopicsAndNamePatternMatches() {
        String raw = "[ { \"level\": \"high\", \"names\": \"Bytes.*\", \"filters\": [{\"topics\":[\"car\",\"bus\"]}] } ]";
        AbstractConfig conf = configWith(raw);
        assertTrue(MetricsVerbosityController.shouldEmitPartitionMetric(conf, "BytesInPerSec", "car"));
        assertTrue(MetricsVerbosityController.shouldEmitPartitionMetric(conf, "BytesOutPerSec", "bus"));
        assertFalse(MetricsVerbosityController.shouldEmitPartitionMetric(conf, "BytesInPerSec", "plane"));
        assertFalse(MetricsVerbosityController.shouldEmitPartitionMetric(conf, "MessagesInPerSec", "car"));
    }

    @Test
    public void testTopicPatternFilterMatches() {
        String raw = "[ { \"level\": \"high\", \"names\": \"Bytes.*\", \"filters\": [{\"topicPattern\": \"t-.*\"}] } ]";
        AbstractConfig conf = configWith(raw);
        assertTrue(MetricsVerbosityController.shouldEmitPartitionMetric(conf, "BytesInPerSec", "t-1"));
        assertFalse(MetricsVerbosityController.shouldEmitPartitionMetric(conf, "BytesInPerSec", "x-1"));
    }

    @Test
    public void testCacheUpdatesWhenConfigChanges() {
        String raw1 = "[ { \"level\": \"high\", \"names\": \"Bytes.*\", \"filters\": [{\"topics\":[\"car\"]}] } ]";
        AbstractConfig conf1 = configWith(raw1);
        assertTrue(MetricsVerbosityController.shouldEmitPartitionMetric(conf1, "BytesInPerSec", "car"));
        assertFalse(MetricsVerbosityController.shouldEmitPartitionMetric(conf1, "BytesInPerSec", "bus"));

        String raw2 = "[ { \"level\": \"high\", \"names\": \"Messages.*\", \"filters\": [{\"topics\":[\"bus\"]}] } ]";
        AbstractConfig conf2 = configWith(raw2);
        // After change, Bytes no longer matches, Messages on bus does
        assertFalse(MetricsVerbosityController.shouldEmitPartitionMetric(conf2, "BytesInPerSec", "car"));
        assertTrue(MetricsVerbosityController.shouldEmitPartitionMetric(conf2, "MessagesInPerSec", "bus"));
    }
}



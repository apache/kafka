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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.PluginMetrics;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class WorkerShareSinkTaskContextTest {

    @Test
    public void rejectsPartitionControlAndErrantRecordReporting() {
        WorkerShareSinkTaskContext context = new WorkerShareSinkTaskContext(Map::of, mock(PluginMetrics.class));
        TopicPartition partition = new TopicPartition("input", 0);

        assertAll(
                () -> assertThrows(UnsupportedOperationException.class, () -> context.offset(Map.of(partition, 1L))),
                () -> assertThrows(UnsupportedOperationException.class, () -> context.offset(partition, 1L)),
                () -> assertThrows(UnsupportedOperationException.class, context::assignment),
                () -> assertThrows(UnsupportedOperationException.class, () -> context.pause(partition)),
                () -> assertThrows(UnsupportedOperationException.class, () -> context.resume(partition)),
                () -> assertThrows(UnsupportedOperationException.class, context::errantRecordReporter)
        );
    }

    @Test
    public void exposesCurrentConfigsAndMetricsAndConsumesTaskLaneHints() {
        Map<String, String> configs = new HashMap<>(Map.of("destination", "initial"));
        PluginMetrics metrics = mock(PluginMetrics.class);
        WorkerShareSinkTaskContext context = new WorkerShareSinkTaskContext(() -> configs, metrics);
        Map<String, String> initialConfigs = context.configs();
        configs.put("destination", "updated");

        assertEquals(Map.of("destination", "initial"), initialConfigs);
        assertEquals(Map.of("destination", "updated"), context.configs());
        assertThrows(UnsupportedOperationException.class, () -> initialConfigs.put("destination", "mutated"));
        assertSame(metrics, context.pluginMetrics());
        assertEquals(new WorkerShareSinkTaskContext.Hints(-1L, false), context.drainHints());

        context.timeout(0L);
        context.requestCommit();
        context.requestCommit();
        assertEquals(new WorkerShareSinkTaskContext.Hints(0L, true), context.drainHints());
        assertEquals(new WorkerShareSinkTaskContext.Hints(-1L, false), context.drainHints());

        context.timeout(10L);
        context.timeout(25L);
        assertThrows(IllegalArgumentException.class, () -> context.timeout(-1L));
        assertEquals(new WorkerShareSinkTaskContext.Hints(25L, false), context.drainHints());
    }
}

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

package org.apache.kafka.coordinator.share.metrics;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.util.Map;

public class ShareCoordinatorMetricsShard implements CoordinatorMetricsShard {

    private final SnapshotRegistry snapshotRegistry;
    private final Map<String, Sensor> globalSensors;
    private final TopicPartition topicPartition;

    public ShareCoordinatorMetricsShard(SnapshotRegistry snapshotRegistry,
                                        Map<String, Sensor> globalSensors,
                                        TopicPartition topicPartition) {
        this.snapshotRegistry = snapshotRegistry;
        this.globalSensors = globalSensors;
        this.topicPartition = topicPartition;
    }

    @Override
    public void record(String sensorName) {
        if (this.globalSensors.containsKey(sensorName)) {
            this.globalSensors.get(sensorName).record();
        }
    }

    @Override
    public void record(String sensorName, double val) {
        if (this.globalSensors.containsKey(sensorName)) {
            this.globalSensors.get(sensorName).record(val);
        }
    }

    @Override
    public TopicPartition topicPartition() {
        return this.topicPartition;
    }

    @Override
    public void commitUpTo(long offset) {

    }
}

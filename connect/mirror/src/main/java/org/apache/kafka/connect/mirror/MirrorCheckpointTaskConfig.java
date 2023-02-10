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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.HashSet;
import java.util.Collections;

public class MirrorCheckpointTaskConfig extends MirrorCheckpointConfig {

    private static final String TASK_CONSUMER_GROUPS_DOC = "Consumer groups assigned to this task to replicate.";

    public MirrorCheckpointTaskConfig(Map<String, String> props) {
        super(TASK_CONFIG_DEF, props);
    }

    Set<String> taskConsumerGroups() {
        List<String> fields = getList(TASK_CONSUMER_GROUPS);
        if (fields == null || fields.isEmpty()) {
            return Collections.emptySet();
        }
        return new HashSet<>(fields);
    }

    MirrorCheckpointMetrics metrics() {
        MirrorCheckpointMetrics metrics = new MirrorCheckpointMetrics(this);
        metricsReporters().forEach(metrics::addReporter);
        return metrics;
    }

    protected static final ConfigDef TASK_CONFIG_DEF = new ConfigDef(CONNECTOR_CONFIG_DEF)
            .define(
                    TASK_CONSUMER_GROUPS,
                    ConfigDef.Type.LIST,
                    null,
                    ConfigDef.Importance.LOW,
                    TASK_CONSUMER_GROUPS_DOC);
}


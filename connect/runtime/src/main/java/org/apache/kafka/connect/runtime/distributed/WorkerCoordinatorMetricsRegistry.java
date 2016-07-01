/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.connect.runtime.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.internals.AbstractCoordinatorMetrics;
import org.apache.kafka.common.MetricNameTemplate;

public class WorkerCoordinatorMetricsRegistry
        extends AbstractCoordinatorMetrics {

    public MetricNameTemplate assignedConnectors;
    public MetricNameTemplate assignedTasks;

    public WorkerCoordinatorMetricsRegistry(String metricGrpPrefix) {
        this(new HashSet<String>(), metricGrpPrefix);
    }

    public WorkerCoordinatorMetricsRegistry(Set<String> metricsTags,
            String metricGrpPrefix) {
        super(metricsTags, metricGrpPrefix);

        String groupName = metricGrpPrefix + "-coordinator-metrics";
        this.assignedConnectors = new MetricNameTemplate("assigned-connectors", groupName,
                "The number of connector instances currently assigned to this consumer", metricsTags);
        this.assignedTasks = new MetricNameTemplate("assigned-tasks", groupName,
                "The number of tasks currently assigned to this consumer", metricsTags);
    }
    
    @Override
    public List<MetricNameTemplate> getAllTemplates() {
        List<MetricNameTemplate> l = new ArrayList<>(super.getAllTemplates());
        l.addAll(Arrays.asList(
                assignedConnectors,
                assignedTasks
            ));
        return l;
    }


}

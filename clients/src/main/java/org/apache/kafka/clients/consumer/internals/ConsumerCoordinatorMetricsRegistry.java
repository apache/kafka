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
package org.apache.kafka.clients.consumer.internals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricNameTemplate;

public class ConsumerCoordinatorMetricsRegistry extends AbstractCoordinatorMetrics {

    private String groupName;
    public MetricNameTemplate commitLatencyAvg;
    public MetricNameTemplate commitLatencyMax;
    public MetricNameTemplate commitRate;
    public MetricNameTemplate assignedPartitions;

    public ConsumerCoordinatorMetricsRegistry(Set<String> metricsTags, String metricGrpPrefix) {
        super(metricsTags, metricGrpPrefix);
        
        this.groupName = metricGrpPrefix + "-coordinator-metrics";
        
        this.commitLatencyAvg = new MetricNameTemplate("commit-latency-avg", groupName,
                "The average time taken for a commit request", metricsTags);
        this.commitLatencyMax = new MetricNameTemplate("commit-latency-max", groupName,
                "The max time taken for a commit request", metricsTags);
        this.commitRate = new MetricNameTemplate("commit-rate", groupName,
                "The number of commit calls per second", metricsTags);
        this.assignedPartitions = new MetricNameTemplate("assigned-partitions", groupName,
                "The number of partitions currently assigned to this consumer", metricsTags);
    }

    public ConsumerCoordinatorMetricsRegistry(String metricGrpPrefix) {
        this(new HashSet<String>(), metricGrpPrefix);
    }
    
    public List<MetricNameTemplate> getAllTemplates() {
        List<MetricNameTemplate> l = new ArrayList<>(super.getAllTemplates());
        l.addAll(Arrays.asList(
                commitLatencyAvg,
                commitLatencyMax,
                commitRate,
                assignedPartitions
            ));
        return l;
    }

}

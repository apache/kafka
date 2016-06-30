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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricNameTemplate;

public class AbstractCoordinatorMetrics {

    public MetricNameTemplate heartbeatResponseTimeMax;
    public MetricNameTemplate heartbeatRate;
    public MetricNameTemplate joinTimeAvg;
    public MetricNameTemplate joinTimeMax;
    public MetricNameTemplate joinRate;
    public MetricNameTemplate syncTimeAvg;
    public MetricNameTemplate syncTimeMax;
    public MetricNameTemplate syncRate;
    public MetricNameTemplate lastHeartbeatSecondsAgo;

    public AbstractCoordinatorMetrics(String metricGrpPrefix) {
        this(new HashSet<String>(), metricGrpPrefix);
    }
    
    public AbstractCoordinatorMetrics(Set<String> metricsTags, String metricGrpPrefix) {
        String groupName = metricGrpPrefix + "-coordinator-metrics";
        
        this.heartbeatResponseTimeMax = new MetricNameTemplate("heartbeat-response-time-max", groupName,
                "The max time taken to receive a response to a heartbeat request", metricsTags);
        this.heartbeatRate = new MetricNameTemplate("heartbeat-rate", groupName,
                "The average number of heartbeats per second", metricsTags);
        this.joinTimeAvg = new MetricNameTemplate("join-time-avg", groupName,
                "The average time taken for a group rejoin", metricsTags);
        this.joinTimeMax = new MetricNameTemplate("join-time-max", groupName,
                "The max time taken for a group rejoin", metricsTags);
        this.joinRate = new MetricNameTemplate("join-rate", groupName,
                "The number of group joins per second", metricsTags);
        this.syncTimeAvg = new MetricNameTemplate("sync-time-avg", groupName,
                "The average time taken for a group sync", metricsTags);
        this.syncTimeMax = new MetricNameTemplate("sync-time-max", groupName,
                "The max time taken for a group sync", metricsTags);
        this.syncRate = new MetricNameTemplate("sync-rate", groupName,
                "The number of group syncs per second", metricsTags);
        this.lastHeartbeatSecondsAgo = new MetricNameTemplate("last-heartbeat-seconds-ago", groupName,
                "The number of seconds since the last controller heartbeat", metricsTags);

    }

    public List<MetricNameTemplate> getAllTemplates() {
        return Arrays.asList(heartbeatResponseTimeMax,
                heartbeatRate,
                joinTimeAvg,
                joinTimeMax,
                joinRate,
                syncTimeAvg,
                syncTimeMax,
                syncRate,
                lastHeartbeatSecondsAgo
            );
    }
    
}

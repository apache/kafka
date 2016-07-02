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
package org.apache.kafka.common.network;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricNameTemplate;

public class SelectorMetricsRegistry {

    public MetricNameTemplate connectionCloseRate;
    public MetricNameTemplate connectionCreationRate;
    public MetricNameTemplate networkIORate;
    public MetricNameTemplate outgoingByteRate;
    public MetricNameTemplate requestRate;
    public MetricNameTemplate requestSizeAvg;
    public MetricNameTemplate requestSizeMax;
    public MetricNameTemplate incomingByteRate;
    public MetricNameTemplate responseRate;
    public MetricNameTemplate selectRate;
    public MetricNameTemplate ioWaitTimeNsAvg;
    public MetricNameTemplate ioWaitRatio;
    public MetricNameTemplate ioTimeNsAvg;
    public MetricNameTemplate ioRatio;
    public MetricNameTemplate connectionCount;
    public MetricNameTemplate nodeOutgoingByteRate;
    public MetricNameTemplate nodeRequestRate;
    public MetricNameTemplate nodeRequestSizeAvg;
    public MetricNameTemplate nodeRequestSizeMax;
    public MetricNameTemplate nodeIncomingByteRate;
    public MetricNameTemplate nodeResponseRate;
    public MetricNameTemplate nodeRequestLatencyAvg;
    public MetricNameTemplate nodeRequestLatencyMax;

    public SelectorMetricsRegistry(String metricGrpPrefix) {
        this(new HashSet<String>(), metricGrpPrefix);
    }

    
    public SelectorMetricsRegistry(Set<String> metricsTags,
            String metricGrpPrefix) {

        String groupName = metricGrpPrefix + "-metrics";
        
        this.connectionCloseRate = new MetricNameTemplate("connection-close-rate", groupName, "Connections closed per second in the window.", metricsTags);
        this.connectionCreationRate = new MetricNameTemplate("connection-creation-rate", groupName, "New connections established per second in the window.", metricsTags);
        this.networkIORate = new MetricNameTemplate("network-io-rate", groupName, "The average number of network operations (reads or writes) on all connections per second.", metricsTags);
        this.outgoingByteRate = new MetricNameTemplate("outgoing-byte-rate", groupName, "The average number of outgoing bytes sent per second to all servers.", metricsTags);
        this.requestRate = new MetricNameTemplate("request-rate", groupName, "The average number of requests sent per second.", metricsTags);

        this.requestSizeAvg = new MetricNameTemplate("request-size-avg", groupName, "The average size of all requests in the window..", metricsTags);
        this.requestSizeMax = new MetricNameTemplate("request-size-max", groupName, "The maximum size of any request sent in the window.", metricsTags);
        this.incomingByteRate = new MetricNameTemplate("incoming-byte-rate", groupName, "Bytes/second read off all sockets", metricsTags);
        this.responseRate = new MetricNameTemplate("response-rate", groupName, "Responses received sent per second.", metricsTags);
        this.selectRate = new MetricNameTemplate("select-rate", groupName, "Number of times the I/O layer checked for new I/O to perform per second", metricsTags);
        this.ioWaitTimeNsAvg = new MetricNameTemplate("io-wait-time-ns-avg", groupName, "The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.", metricsTags);
        this.ioWaitRatio = new MetricNameTemplate("io-wait-ratio", groupName, "The fraction of time the I/O thread spent waiting.", metricsTags);
        this.ioTimeNsAvg = new MetricNameTemplate("io-time-ns-avg", groupName, "The average length of time for I/O per select call in nanoseconds.", metricsTags);
        this.ioRatio = new MetricNameTemplate("io-ratio", groupName, "The fraction of time the I/O thread spent doing I/O", metricsTags);
        this.connectionCount = new MetricNameTemplate("connection-count", groupName, "The current number of active connections.", metricsTags);

        String nodeGroupName = metricGrpPrefix + "-node-metrics";
        
        Set<String> nodeGroupTags = new HashSet<>(metricsTags);
        nodeGroupTags.add("node-id");
        
        this.nodeOutgoingByteRate = new MetricNameTemplate("outgoing-byte-rate", nodeGroupName, "The average number of outgoing bytes sent per second to all servers.", nodeGroupTags);
        this.nodeRequestRate = new MetricNameTemplate("request-rate", nodeGroupName, "The average number of requests sent per second.", nodeGroupTags);
        this.nodeRequestSizeAvg = new MetricNameTemplate("request-size-avg", nodeGroupName, "The average size of all requests in the window..", nodeGroupTags);
        this.nodeRequestSizeMax = new MetricNameTemplate("request-size-max", nodeGroupName, "The maximum size of any request sent in the window.", nodeGroupTags);

        this.nodeIncomingByteRate = new MetricNameTemplate("incoming-byte-rate", nodeGroupName, "Bytes/second read off all sockets", nodeGroupTags);
        this.nodeResponseRate = new MetricNameTemplate("response-rate", nodeGroupName, "The average number of responses received per second.", nodeGroupTags);

        this.nodeRequestLatencyAvg = new MetricNameTemplate("request-latency-avg", nodeGroupName, "The average request latency in ms", nodeGroupTags);
        this.nodeRequestLatencyMax = new MetricNameTemplate("request-latency-max", nodeGroupName, "The maximum request latency in ms", nodeGroupTags);

    }


    public List<MetricNameTemplate> getAllTemplates() {
        return Arrays.asList(this.connectionCloseRate,
                this.connectionCreationRate,
                this.networkIORate,
                this.outgoingByteRate,
                this.requestRate,
                this.requestSizeAvg,
                this.requestSizeMax,
                this.incomingByteRate,
                this.responseRate,
                this.selectRate,
                this.ioWaitTimeNsAvg,
                this.ioWaitRatio,
                this.ioTimeNsAvg,
                this.ioRatio,
                this.connectionCount,
                this.nodeOutgoingByteRate,
                this.nodeRequestRate,
                this.nodeRequestSizeAvg,
                this.nodeRequestSizeMax,
                this.nodeIncomingByteRate,
                this.nodeResponseRate,
                this.nodeRequestLatencyAvg,
                this.nodeRequestLatencyMax
            );
    }

}

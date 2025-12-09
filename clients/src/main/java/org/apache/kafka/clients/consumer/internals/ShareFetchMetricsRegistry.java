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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.MetricNameTemplate;

import java.util.HashSet;
import java.util.Set;

public class ShareFetchMetricsRegistry {

    public MetricNameTemplate fetchSizeAvg;
    public MetricNameTemplate fetchSizeMax;
    public MetricNameTemplate bytesFetchedRate;
    public MetricNameTemplate bytesFetchedTotal;
    public MetricNameTemplate recordsPerRequestAvg;
    public MetricNameTemplate recordsPerRequestMax;
    public MetricNameTemplate recordsFetchedRate;
    public MetricNameTemplate recordsFetchedTotal;
    public MetricNameTemplate acknowledgementSendRate;
    public MetricNameTemplate acknowledgementSendTotal;
    public MetricNameTemplate acknowledgementErrorRate;
    public MetricNameTemplate acknowledgementErrorTotal;
    public MetricNameTemplate fetchLatencyAvg;
    public MetricNameTemplate fetchLatencyMax;
    public MetricNameTemplate fetchRequestRate;
    public MetricNameTemplate fetchRequestTotal;
    public MetricNameTemplate fetchThrottleTimeAvg;
    public MetricNameTemplate fetchThrottleTimeMax;

    public ShareFetchMetricsRegistry() {
        this(new HashSet<>(), "");
    }

    public ShareFetchMetricsRegistry(String metricGrpPrefix) {
        this(new HashSet<>(), metricGrpPrefix);
    }

    public ShareFetchMetricsRegistry(Set<String> tags, String metricGrpPrefix) {

        /* Client level */
        String groupName = metricGrpPrefix + "-fetch-manager-metrics";

        this.fetchSizeAvg = new MetricNameTemplate("fetch-size-avg", groupName,
                "The average number of bytes fetched per request", tags);

        this.fetchSizeMax = new MetricNameTemplate("fetch-size-max", groupName,
                "The maximum number of bytes fetched per request", tags);
        this.bytesFetchedRate = new MetricNameTemplate("bytes-consumed-rate", groupName,
                "The average number of bytes consumed per second", tags);
        this.bytesFetchedTotal = new MetricNameTemplate("bytes-consumed-total", groupName,
                "The total number of bytes consumed", tags);

        this.recordsPerRequestAvg = new MetricNameTemplate("records-per-request-avg", groupName,
                "The average number of records in each request", tags);
        this.recordsPerRequestMax = new MetricNameTemplate("records-per-request-max", groupName,
                "The maximum number of records in a request.", tags);
        this.recordsFetchedRate = new MetricNameTemplate("records-consumed-rate", groupName,
                "The average number of records consumed per second", tags);
        this.recordsFetchedTotal = new MetricNameTemplate("records-consumed-total", groupName,
                "The total number of records consumed", tags);

        this.acknowledgementSendRate = new MetricNameTemplate("acknowledgements-send-rate", groupName,
                "The average number of record acknowledgements sent per second.", tags);
        this.acknowledgementSendTotal = new MetricNameTemplate("acknowledgements-send-total", groupName,
                "The total number of record acknowledgements sent.", tags);
        this.acknowledgementErrorRate = new MetricNameTemplate("acknowledgements-error-rate", groupName,
                "The average number of record acknowledgements that resulted in errors per second.", tags);
        this.acknowledgementErrorTotal = new MetricNameTemplate("acknowledgements-error-total", groupName,
                "The total number of record acknowledgements that resulted in errors.", tags);

        this.fetchLatencyAvg = new MetricNameTemplate("fetch-latency-avg", groupName,
                "The average time taken for a fetch request.", tags);
        this.fetchLatencyMax = new MetricNameTemplate("fetch-latency-max", groupName,
                "The max time taken for any fetch request.", tags);
        this.fetchRequestRate = new MetricNameTemplate("fetch-rate", groupName,
                "The number of fetch requests per second.", tags);
        this.fetchRequestTotal = new MetricNameTemplate("fetch-total", groupName,
                "The total number of fetch requests.", tags);


        this.fetchThrottleTimeAvg = new MetricNameTemplate("fetch-throttle-time-avg", groupName,
                "The average throttle time in ms", tags);
        this.fetchThrottleTimeMax = new MetricNameTemplate("fetch-throttle-time-max", groupName,
                "The maximum throttle time in ms", tags);
    }
}

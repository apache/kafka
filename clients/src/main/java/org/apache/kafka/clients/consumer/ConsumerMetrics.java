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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.SpecificMetrics;

public class ConsumerMetrics {

    // XXX group name is no longer configurable at runtime. Is that okay?
    public static final MetricNameTemplate FETCH_SIZE_AVG = new MetricNameTemplate("fetch-size-avg", "consumer-fetch-manager-metrics", 
            "The average number of bytes fetched per request", "client-id");
    public static final MetricNameTemplate FETCH_SIZE_MAX = new MetricNameTemplate("fetch-size-max", "consumer-fetch-manager-metrics", 
            "The maximum number of bytes fetched per request", "client-id");
    public static final MetricNameTemplate BYTES_CONSUMED_RATE = new MetricNameTemplate("bytes-consumed-rate", "consumer-fetch-manager-metrics", 
            "The average number of bytes consumed per second", "client-id");

    public static final MetricNameTemplate RECORDS_PER_REQUEST_AVG = new MetricNameTemplate("records-per-request-avg", "consumer-fetch-manager-metrics", 
            "The average number of records in each request", "client-id");
    public static final MetricNameTemplate RECORDS_CONSUMED_RATE = new MetricNameTemplate("records-consumed-rate", "consumer-fetch-manager-metrics", 
            "The average number of records consumed per second", "client-id");

    public static final MetricNameTemplate FETCH_LATENCY_AVG = new MetricNameTemplate("fetch-latency-avg", "consumer-fetch-manager-metrics", 
            "The average time taken for a fetch request.", "client-id");
    public static final MetricNameTemplate FETCH_LATENCY_MAX = new MetricNameTemplate("fetch-latency-max", "consumer-fetch-manager-metrics", 
            "The max time taken for any fetch request.", "client-id");
    public static final MetricNameTemplate FETCH_RATE = new MetricNameTemplate("fetch-rate", "consumer-fetch-manager-metrics", 
            "The number of fetch requests per second.", "client-id");

    public static final MetricNameTemplate RECORDS_LAG_MAX = new MetricNameTemplate("records-lag-max", "consumer-fetch-manager-metrics", 
            "The maximum lag in terms of number of records for any partition in this window", "client-id");

    public static final MetricNameTemplate FETCH_THROTTLE_TIME_AVG = new MetricNameTemplate("fetch-throttle-time-avg", "consumer-fetch-manager-metrics", 
            "The average throttle time in ms", "client-id");
    public static final MetricNameTemplate FETCH_THROTTLE_TIME_MAX = new MetricNameTemplate("fetch-throttle-time-max", "consumer-fetch-manager-metrics", 
            "The maximum throttle time in ms", "client-id");

    // XXX can I put the topic name in the JMX description?
    public static final MetricNameTemplate TOPIC_FETCH_SIZE_AVG = new MetricNameTemplate("fetch-size-avg", "consumer-fetch-manager-metrics", 
            "The average number of bytes fetched per request for topic {topic}", "client-id", "topic");
    public static final MetricNameTemplate TOPIC_FETCH_SIZE_MAX = new MetricNameTemplate("fetch-size-max", "consumer-fetch-manager-metrics", 
            "The maximum number of bytes fetched per request for topic {topic}", "client-id", "topic");
    public static final MetricNameTemplate TOPIC_BYTES_CONSUMED_RATE = new MetricNameTemplate("bytes-consumed-rate", "consumer-fetch-manager-metrics", 
            "The average number of bytes consumed per second for topic {topic}", "client-id", "topic");

    public static final MetricNameTemplate TOPIC_RECORDS_PER_REQUEST_AVG = new MetricNameTemplate("records-per-request-avg", "consumer-fetch-manager-metrics", 
            "The average number of records in each request for topic {topic}", "client-id", "topic");
    public static final MetricNameTemplate TOPIC_RECORDS_CONSUMED_RATE = new MetricNameTemplate("records-consumed-rate", "consumer-fetch-manager-metrics", 
            "The average number of records consumed per second for topic {topic}", "client-id", "topic");

    private static final MetricNameTemplate[] ALL_METRICS = {
        FETCH_SIZE_AVG,
        FETCH_SIZE_MAX,
        BYTES_CONSUMED_RATE,
        RECORDS_PER_REQUEST_AVG,
        RECORDS_CONSUMED_RATE,
        FETCH_LATENCY_AVG,
        FETCH_LATENCY_MAX,
        FETCH_RATE,
        RECORDS_LAG_MAX,
        FETCH_THROTTLE_TIME_AVG,
        FETCH_THROTTLE_TIME_MAX,
        TOPIC_FETCH_SIZE_AVG,
        TOPIC_FETCH_SIZE_MAX,
        TOPIC_BYTES_CONSUMED_RATE,
        TOPIC_RECORDS_PER_REQUEST_AVG,
        TOPIC_RECORDS_CONSUMED_RATE,
    };
    
    public static void main(String[] args) {
        System.out.println(SpecificMetrics.toHtmlTable("kafka.consumer", ALL_METRICS));
    }

}

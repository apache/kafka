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
 */package org.apache.kafka.clients.consumer.internals;

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.common.MetricNameTemplate;

public class FetcherMetricsRegistry {

    private Set<String> tags;
    private Set<String> topicTags;
    public MetricNameTemplate FETCH_SIZE_AVG;
    public MetricNameTemplate FETCH_SIZE_MAX;
    public MetricNameTemplate BYTES_CONSUMED_RATE;
    public MetricNameTemplate RECORDS_PER_REQUEST_AVG;
    public MetricNameTemplate RECORDS_CONSUMED_RATE;
    public MetricNameTemplate FETCH_LATENCY_AVG;
    public MetricNameTemplate FETCH_LATENCY_MAX;
    public MetricNameTemplate FETCH_RATE;
    public MetricNameTemplate RECORDS_LAG_MAX;
    public MetricNameTemplate FETCH_THROTTLE_TIME_AVG;
    public MetricNameTemplate FETCH_THROTTLE_TIME_MAX;
    public MetricNameTemplate TOPIC_FETCH_SIZE_AVG;
    public MetricNameTemplate TOPIC_FETCH_SIZE_MAX;
    public MetricNameTemplate TOPIC_BYTES_CONSUMED_RATE;
    public MetricNameTemplate TOPIC_RECORDS_PER_REQUEST_AVG;
    public MetricNameTemplate TOPIC_RECORDS_CONSUMED_RATE;

    public FetcherMetricsRegistry() {
        this(new HashSet<String>());
    }

    public FetcherMetricsRegistry(Set<String> tagsArg) {
        // make a copuy
        this.tags = new HashSet<>(tagsArg);
        topicTags = new HashSet<>(tagsArg);
        topicTags.add("topic");

        /* ***** Fetcher level *****/
        this.FETCH_SIZE_AVG = new MetricNameTemplate("fetch-size-avg", "consumer-fetch-manager-metrics", 
                "The average number of bytes fetched per request", tags);

        this.FETCH_SIZE_MAX = new MetricNameTemplate("fetch-size-max", "consumer-fetch-manager-metrics", 
                "The maximum number of bytes fetched per request", tags);
        this.BYTES_CONSUMED_RATE = new MetricNameTemplate("bytes-consumed-rate", "consumer-fetch-manager-metrics", 
                "The average number of bytes consumed per second", tags);

        this.RECORDS_PER_REQUEST_AVG = new MetricNameTemplate("records-per-request-avg", "consumer-fetch-manager-metrics", 
                "The average number of records in each request", tags);
        this.RECORDS_CONSUMED_RATE = new MetricNameTemplate("records-consumed-rate", "consumer-fetch-manager-metrics", 
                "The average number of records consumed per second", tags);

        this.FETCH_LATENCY_AVG = new MetricNameTemplate("fetch-latency-avg", "consumer-fetch-manager-metrics", 
                "The average time taken for a fetch request.", tags);
        this.FETCH_LATENCY_MAX = new MetricNameTemplate("fetch-latency-max", "consumer-fetch-manager-metrics", 
                "The max time taken for any fetch request.", tags);
        this.FETCH_RATE = new MetricNameTemplate("fetch-rate", "consumer-fetch-manager-metrics", 
                "The number of fetch requests per second.", tags);

        this.RECORDS_LAG_MAX = new MetricNameTemplate("records-lag-max", "consumer-fetch-manager-metrics", 
                "The maximum lag in terms of number of records for any partition in this window", tags);

        this.FETCH_THROTTLE_TIME_AVG = new MetricNameTemplate("fetch-throttle-time-avg", "consumer-fetch-manager-metrics", 
                "The average throttle time in ms", tags);
        this.FETCH_THROTTLE_TIME_MAX = new MetricNameTemplate("fetch-throttle-time-max", "consumer-fetch-manager-metrics", 
                "The maximum throttle time in ms", tags);

        /* ***** Topic level *****/
        this.TOPIC_FETCH_SIZE_AVG = new MetricNameTemplate("fetch-size-avg", "consumer-fetch-manager-metrics", 
                "The average number of bytes fetched per request for topic {topic}", topicTags);
        this.TOPIC_FETCH_SIZE_MAX = new MetricNameTemplate("fetch-size-max", "consumer-fetch-manager-metrics", 
                "The maximum number of bytes fetched per request for topic {topic}", topicTags);
        this.TOPIC_BYTES_CONSUMED_RATE = new MetricNameTemplate("bytes-consumed-rate", "consumer-fetch-manager-metrics", 
                "The average number of bytes consumed per second for topic {topic}", topicTags);

        this.TOPIC_RECORDS_PER_REQUEST_AVG = new MetricNameTemplate("records-per-request-avg", "consumer-fetch-manager-metrics", 
                "The average number of records in each request for topic {topic}", topicTags);
        this.TOPIC_RECORDS_CONSUMED_RATE = new MetricNameTemplate("records-consumed-rate", "consumer-fetch-manager-metrics", 
                "The average number of records consumed per second for topic {topic}", topicTags);
        
    }
    
    public MetricNameTemplate[] getAllTemplates() {
        return new MetricNameTemplate[] {
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
    }

}

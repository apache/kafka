package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.MetricNameTemplate;

public class ConsumerMetrics {

    // XXX group name is no longer configurable at runtime. Is that okay?
    public static final MetricNameTemplate FETCH_SIZE_AVG = new MetricNameTemplate("fetch-size-avg", "consumer-fetch-manager-metrics", 
            "The average number of bytes fetched per request");
    public static final MetricNameTemplate FETCH_SIZE_MAX = new MetricNameTemplate("fetch-size-max", "consumer-fetch-manager-metrics", 
            "The maximum number of bytes fetched per request");
    public static final MetricNameTemplate BYTES_CONSUMED_RATE = new MetricNameTemplate("bytes-consumed-rate", "consumer-fetch-manager-metrics", 
            "The average number of bytes consumed per second");

    public static final MetricNameTemplate RECORDS_PER_REQUEST_AVG = new MetricNameTemplate("records-per-request-avg", "consumer-fetch-manager-metrics", 
            "The average number of records in each request");
    public static final MetricNameTemplate RECORDS_CONSUMED_RATE = new MetricNameTemplate("records-consumed-rate", "consumer-fetch-manager-metrics", 
            "The average number of records consumed per second");

    public static final MetricNameTemplate FETCH_LATENCY_AVG = new MetricNameTemplate("fetch-latency-avg", "consumer-fetch-manager-metrics", 
            "The average time taken for a fetch request.");
    public static final MetricNameTemplate FETCH_LATENCY_MAX = new MetricNameTemplate("fetch-latency-max", "consumer-fetch-manager-metrics", 
            "The max time taken for any fetch request.");
    public static final MetricNameTemplate FETCH_RATE = new MetricNameTemplate("fetch-rate", "consumer-fetch-manager-metrics", 
            "The number of fetch requests per second.");

    public static final MetricNameTemplate RECORDS_LAG_MAX = new MetricNameTemplate("records-lag-max", "consumer-fetch-manager-metrics", 
            "The maximum lag in terms of number of records for any partition in this window");

    public static final MetricNameTemplate FETCH_THROTTLE_TIME_AVG = new MetricNameTemplate("fetch-throttle-time-avg", "consumer-fetch-manager-metrics", 
            "The average throttle time in ms");
    public static final MetricNameTemplate FETCH_THROTTLE_TIME_MAX = new MetricNameTemplate("fetch-throttle-time-max", "consumer-fetch-manager-metrics", 
            "The maximum throttle time in ms");

    // XXX can I put the topic name in the JMX description?
    public static final MetricNameTemplate TOPIC_FETCH_SIZE_AVG = new MetricNameTemplate("fetch-size-avg", "consumer-fetch-manager-metrics", 
            "The average number of bytes fetched per request for topic {topic}", "topic");
    public static final MetricNameTemplate TOPIC_FETCH_SIZE_MAX = new MetricNameTemplate("fetch-size-max", "consumer-fetch-manager-metrics", 
            "The maximum number of bytes fetched per request for topic {topic}", "topic");
    public static final MetricNameTemplate TOPIC_BYTES_CONSUMED_RATE = new MetricNameTemplate("bytes-consumed-rate", "consumer-fetch-manager-metrics", 
            "The average number of bytes consumed per second for topic {topic}", "topic");

    public static final MetricNameTemplate TOPIC_RECORDS_PER_REQUEST_AVG = new MetricNameTemplate("records-per-request-avg", "consumer-fetch-manager-metrics", 
            "The average number of records in each request for topic {topic}", "topic");
    public static final MetricNameTemplate TOPIC_RECORDS_CONSUMED_RATE = new MetricNameTemplate("records-consumed-rate", "consumer-fetch-manager-metrics", 
            "The average number of records consumed per second for topic {topic}", "topic");

    
}

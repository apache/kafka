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
package org.apache.kafka.tools;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * MetricsReporter that aggregates metrics data and reports it via HTTP requests to a configurable
 * webhook endpoint in JSON format.
 */
public class HttpMetricsReporter implements MetricsReporter {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricsReporter.class);

    private static final String METRICS_PREFIX = "metrics.";
    private static final String METRICS_URL_CONFIG = METRICS_PREFIX + "url";
    private static final String METRICS_PERIOD_CONFIG = METRICS_PREFIX + "period";
    private static final String METRICS_HOST_CONFIG = METRICS_PREFIX + "host";
    private static final String CLIENT_ID_CONFIG = ProducerConfig.CLIENT_ID_CONFIG;

    private static final Map<String, String> HEADERS = new LinkedHashMap<>();
    static {
        HEADERS.put("Content-Type", "application/json");
    }

    private final Object lock = new Object();
    private ScheduledExecutorService executor;
    private URL url;
    private String host;
    private String clientId;
    private final Map<MetricName, KafkaMetric> metrics = new HashMap<>();
    private final ObjectMapper json = new ObjectMapper();

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(METRICS_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "The URL to report metrics to")
            .define(METRICS_PERIOD_CONFIG, ConfigDef.Type.INT, ConfigDef.Importance.HIGH,
                    "The frequency at which metrics should be reported, in second")
            .define(METRICS_HOST_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                    "The hostname to report with each metric; if null, defaults to the FQDN that can be automatically" +
                            "determined")
            .define(CLIENT_ID_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.LOW,
                    "Client ID to identify the application, generally inherited from the " +
                            "producer/consumer/streams/connect instance");

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs, true) { };
        try {
            url = new URL(config.getString(METRICS_URL_CONFIG));
        } catch (MalformedURLException e) {
            throw new ConfigException("Malformed metrics.url", e);
        }
        int period = config.getInt(METRICS_PERIOD_CONFIG);
        clientId = config.getString(CLIENT_ID_CONFIG);

        host = config.getString(METRICS_HOST_CONFIG);
        if (host == null) {
            try {
                host = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                throw new ConfigException("Failed to get canonical hostname", e);
            }
        }

        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(new HttpReporter(), period, period, TimeUnit.SECONDS);

        log.info("Configured HttpMetricsReporter for {} to report every {} seconds", url, period);
    }

    @Override
    public void init(List<KafkaMetric> initMetrics) {
        synchronized (lock) {
            for (KafkaMetric metric : initMetrics) {
                log.debug("Adding metric {}", metric.metricName());
                metrics.put(metric.metricName(), metric);
            }
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        synchronized (lock) {
            log.debug("Updating metric {}", metric.metricName());
            metrics.put(metric.metricName(), metric);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        synchronized (lock) {
            log.debug("Removing metric {}", metric.metricName());
            metrics.remove(metric.metricName());
        }
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new InterruptException("Interrupted when shutting down HttpMetricsReporter", e);
        }
    }

    private class HttpReporter implements Runnable {
        @Override
        public void run() {
            long now = Time.SYSTEM.milliseconds();
            final List<MetricValue> samples;
            synchronized (lock) {
                samples = new ArrayList<>(metrics.size());
                for (KafkaMetric metric : metrics.values()) {
                    MetricName name = metric.metricName();
                    double value = metric.value();
                    samples.add(new MetricValue(name.name(), name.group(), name.tags(), value));
                }
            }

            MetricsReport report = new MetricsReport(new MetricClientInfo(host, clientId, now), samples);

            log.trace("Reporting {} metrics to {}", samples.size(), url);
            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                // connection.getResponseCode() implicitly calls getInputStream, so always set to true.
                // On the other hand, leaving this out breaks nothing.
                connection.setDoInput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                byte[] data = json.writeValueAsBytes(report);
                connection.setRequestProperty("Content-Length", Integer.toString(data.length));
                connection.setRequestProperty("Accept", "*/*");
                connection.setUseCaches(false);

                connection.setDoOutput(true);
                OutputStream os = null;
                try {
                    os = connection.getOutputStream();
                    os.write(data);
                    os.flush();
                } finally {
                    if (os != null) os.close();
                }

                int responseCode = connection.getResponseCode();
                if (responseCode >= 400) {
                    InputStream is = connection.getErrorStream();
                    JsonNode msg = json.readTree(is);
                    is.close();
                    log.error("Error reporting metrics, {}: {}", responseCode, msg);
                } else if (responseCode >= 300) {
                    log.error("HttpMetricsReporter does not currently support redirects, saw {}", responseCode);
                } else {
                    log.info("Finished reporting metrics with response code {}", responseCode);
                }
            } catch (Exception e) {
                log.error("Error reporting metrics", e);
                throw new KafkaException("Failed to report current metrics", e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }
    }

    private static class MetricsReport {
        private final MetricClientInfo client;
        private final Collection<MetricValue> metrics;

        MetricsReport(MetricClientInfo client, Collection<MetricValue> metrics) {
            this.client = client;
            this.metrics = metrics;
        }

        @JsonProperty
        public MetricClientInfo client() {
            return client;
        }

        @JsonProperty
        public Collection<MetricValue> metrics() {
            return metrics;
        }
    }

    private static class MetricClientInfo {
        private final String host;
        private final String clientId;
        private final long time;

        MetricClientInfo(String host, String clientId, long time) {
            this.host = host;
            this.clientId = clientId;
            this.time = time;
        }

        @JsonProperty
        public String host() {
            return host;
        }

        @JsonProperty("client_id")
        public String clientId() {
            return clientId;
        }

        @JsonProperty
        public long time() {
            return time;
        }
    }

    private static class MetricValue {

        private final String name;
        private final String group;
        private final Map<String, String> tags;
        private final Object value;

        MetricValue(String name, String group, Map<String, String> tags, Object value) {
            this.name = name;
            this.group = group;
            this.tags = tags;
            this.value = value;
        }

        @JsonProperty
        public String name() {
            return name;
        }

        @JsonProperty
        public String group() {
            return group;
        }

        @JsonProperty
        public Map<String, String> tags() {
            return tags;
        }

        @JsonProperty
        public Object value() {
            return value;
        }
    }
}

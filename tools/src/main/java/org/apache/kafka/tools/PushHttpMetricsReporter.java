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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * MetricsReporter that aggregates metrics data and reports it via HTTP requests to a configurable
 * webhook endpoint in JSON format.
 *
 * This is an internal class used for system tests and does not provide any compatibility guarantees.
 */
public class PushHttpMetricsReporter implements MetricsReporter {
    private static final Logger log = LoggerFactory.getLogger(PushHttpMetricsReporter.class);

    private static final String METRICS_PREFIX = "metrics.";
    static final String METRICS_URL_CONFIG = METRICS_PREFIX + "url";
    static final String METRICS_PERIOD_CONFIG = METRICS_PREFIX + "period";
    static final String METRICS_HOST_CONFIG = METRICS_PREFIX + "host";
    static final String CLIENT_ID_CONFIG = ProducerConfig.CLIENT_ID_CONFIG;

    private static final Map<String, String> HEADERS = new LinkedHashMap<>();
    static {
        HEADERS.put("Content-Type", "application/json");
    }

    private final Object lock = new Object();
    private final Time time;
    private final ScheduledExecutorService executor;
    // The set of metrics are updated in init/metricChange/metricRemoval
    private final Map<MetricName, KafkaMetric> metrics = new LinkedHashMap<>();
    private final ObjectMapper json = new ObjectMapper();

    // Non-final because these are set via configure()
    private URL url;
    private String host;
    private String clientId;

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(METRICS_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "The URL to report metrics to")
            .define(METRICS_PERIOD_CONFIG, ConfigDef.Type.INT, ConfigDef.Importance.HIGH,
                    "The frequency at which metrics should be reported, in second")
            .define(METRICS_HOST_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                    "The hostname to report with each metric; if empty, defaults to the FQDN that can be automatically" +
                            "determined")
            .define(CLIENT_ID_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                    "Client ID to identify the application, generally inherited from the " +
                            "producer/consumer/streams/connect instance");

    public PushHttpMetricsReporter() {
        time = new SystemTime();
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    PushHttpMetricsReporter(Time mockTime, ScheduledExecutorService mockExecutor) {
        time = mockTime;
        executor = mockExecutor;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        PushHttpMetricsReporterConfig config = new PushHttpMetricsReporterConfig(CONFIG_DEF, configs);
        try {
            url = new URL(config.getString(METRICS_URL_CONFIG));
        } catch (MalformedURLException e) {
            throw new ConfigException("Malformed metrics.url", e);
        }
        int period = config.getInteger(METRICS_PERIOD_CONFIG);
        clientId = config.getString(CLIENT_ID_CONFIG);

        host = config.getString(METRICS_HOST_CONFIG);
        if (host == null || host.isEmpty()) {
            try {
                host = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                throw new ConfigException("Failed to get canonical hostname", e);
            }
        }

        executor.scheduleAtFixedRate(new HttpReporter(), period, period, TimeUnit.SECONDS);

        log.info("Configured PushHttpMetricsReporter for {} to report every {} seconds", url, period);
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
            throw new KafkaException("Interrupted when shutting down PushHttpMetricsReporter", e);
        }
    }

    private class HttpReporter implements Runnable {
        @Override
        public void run() {
            long now = time.milliseconds();
            final List<MetricValue> samples;
            synchronized (lock) {
                samples = new ArrayList<>(metrics.size());
                for (KafkaMetric metric : metrics.values()) {
                    MetricName name = metric.metricName();
                    samples.add(new MetricValue(name.name(), name.group(), name.tags(), metric.metricValue()));
                }
            }

            MetricsReport report = new MetricsReport(new MetricClientInfo(host, clientId, now), samples);

            log.trace("Reporting {} metrics to {}", samples.size(), url);
            HttpURLConnection connection = null;
            try {
                connection = newHttpConnection(url);
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

                try (OutputStream os = connection.getOutputStream()) {
                    os.write(data);
                    os.flush();
                }

                int responseCode = connection.getResponseCode();
                if (responseCode >= 400) {
                    InputStream is = connection.getErrorStream();
                    String msg = readResponse(is);
                    log.error("Error reporting metrics, {}: {}", responseCode, msg);
                } else if (responseCode >= 300) {
                    log.error("PushHttpMetricsReporter does not currently support redirects, saw {}", responseCode);
                } else {
                    log.info("Finished reporting metrics with response code {}", responseCode);
                }
            } catch (Throwable t) {
                log.error("Error reporting metrics", t);
                throw new KafkaException("Failed to report current metrics", t);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }
    }

    // Static package-private so unit tests can use a mock connection
    static HttpURLConnection newHttpConnection(URL url) throws IOException {
        return (HttpURLConnection) url.openConnection();
    }

    // Static package-private so unit tests can mock reading response
    static String readResponse(InputStream is) {
        try (Scanner s = new Scanner(is, StandardCharsets.UTF_8.name()).useDelimiter("\\A")) {
            return s.hasNext() ? s.next() : "";
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

    // The signature for getInt changed from returning int to Integer so to remain compatible with 0.8.2.2 jars
    // for system tests we replace it with a custom version that works for all versions.
    private static class PushHttpMetricsReporterConfig extends AbstractConfig {
        public PushHttpMetricsReporterConfig(ConfigDef definition, Map<?, ?> originals) {
            super(definition, originals);
        }

        public Integer getInteger(String key) {
            return (Integer) get(key);
        }

    }
}

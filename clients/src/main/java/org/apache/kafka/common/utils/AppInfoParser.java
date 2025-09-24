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
package org.apache.kafka.common.utils;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Properties;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public class AppInfoParser {
    private static final Logger log = LoggerFactory.getLogger(AppInfoParser.class);
    private static final String VERSION;
    private static final String COMMIT_ID;

    protected static final String DEFAULT_VALUE = "unknown";

    static {
        Properties props = new Properties();
        try (InputStream resourceStream = AppInfoParser.class.getResourceAsStream("/kafka/kafka-version.properties")) {
            props.load(resourceStream);
        } catch (Exception e) {
            log.warn("Error while loading kafka-version.properties: {}", e.getMessage());
        }
        VERSION = props.getProperty("version", DEFAULT_VALUE).trim();
        COMMIT_ID = props.getProperty("commitId", DEFAULT_VALUE).trim();
    }

    public static String getVersion() {
        return VERSION;
    }

    public static String getCommitId() {
        return COMMIT_ID;
    }

    public static synchronized void registerAppInfo(String prefix, String id, Metrics metrics, long nowMs) {
        try {
            ObjectName name = new ObjectName(prefix + ":type=app-info,id=" + Sanitizer.jmxSanitize(id));
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            if (server.isRegistered(name)) {
                log.info("The mbean of App info: [{}], id: [{}] already exists, so skipping a new mbean creation.", prefix, id);
                return;
            }
            AppInfo mBean = new AppInfo(nowMs);
            server.registerMBean(mBean, name);

            registerMetrics(metrics, mBean, id); // prefix will be added later by JmxReporter
        } catch (JMException e) {
            log.warn("Error registering AppInfo mbean", e);
        }
    }

    public static synchronized void unregisterAppInfo(String prefix, String id, Metrics metrics) {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName(prefix + ":type=app-info,id=" + Sanitizer.jmxSanitize(id));
            if (server.isRegistered(name))
                server.unregisterMBean(name);

            unregisterMetrics(metrics, id);
        } catch (JMException e) {
            log.warn("Error unregistering AppInfo mbean", e);
        } finally {
            log.info("App info {} for {} unregistered", prefix, id);
        }
    }

    private static MetricName metricName(Metrics metrics, String name, Map<String, String> tags) {
        return metrics.metricName(name, "app-info", "Metric indicating " + name, tags);
    }

    private static void registerMetrics(Metrics metrics, AppInfo appInfo, String clientId) {
        if (metrics == null) return;
        // Most Kafka clients (producer/consumer/admin) set the client-id tag in the metrics config.
        // Although we don’t explicitly parse client-id here, these metrics are automatically tagged with client-id.
        metrics.addMetric(metricName(metrics, "version", Map.of()), (Gauge<String>) (config, now) -> appInfo.getVersion());
        metrics.addMetric(metricName(metrics, "commit-id", Map.of()), (Gauge<String>) (config, now) -> appInfo.getCommitId());
        metrics.addMetric(metricName(metrics, "start-time-ms", Map.of()), (Gauge<Long>) (config, now) -> appInfo.getStartTimeMs());
        // MirrorMaker/Worker doesn't set client-id tag into the metrics config, so we need to set it here.
        if (!metrics.config().tags().containsKey("client-id") && clientId != null) {
            metrics.addMetric(metricName(metrics, "version", Map.of("client-id", clientId)), (Gauge<String>) (config, now) -> appInfo.getVersion());
            metrics.addMetric(metricName(metrics, "commit-id", Map.of("client-id", clientId)), (Gauge<String>) (config, now) -> appInfo.getCommitId());
            metrics.addMetric(metricName(metrics, "start-time-ms", Map.of("client-id", clientId)), (Gauge<Long>) (config, now) -> appInfo.getStartTimeMs());
        }
    }

    private static void unregisterMetrics(Metrics metrics, String clientId) {
        if (metrics == null) return;

        metrics.removeMetric(metricName(metrics, "version", Map.of()));
        metrics.removeMetric(metricName(metrics, "commit-id", Map.of()));
        metrics.removeMetric(metricName(metrics, "start-time-ms", Map.of()));

        if (!metrics.config().tags().containsKey("client-id") && clientId != null) {
            metrics.removeMetric(metricName(metrics, "version", Map.of("client-id", clientId)));
            metrics.removeMetric(metricName(metrics, "commit-id", Map.of("client-id", clientId)));
            metrics.removeMetric(metricName(metrics, "start-time-ms", Map.of("client-id", clientId)));
        }
    }

    public interface AppInfoMBean {
        String getVersion();
        String getCommitId();
        Long getStartTimeMs();
    }

    public static class AppInfo implements AppInfoMBean {

        private final Long startTimeMs;

        public AppInfo(long startTimeMs) {
            this.startTimeMs = startTimeMs;
            log.info("Kafka version: {}", AppInfoParser.getVersion());
            log.info("Kafka commitId: {}", AppInfoParser.getCommitId());
            log.info("Kafka startTimeMs: {}", startTimeMs);
        }

        @Override
        public String getVersion() {
            return AppInfoParser.getVersion();
        }

        @Override
        public String getCommitId() {
            return AppInfoParser.getCommitId();
        }

        @Override
        public Long getStartTimeMs() {
            return startTimeMs;
        }

    }
}

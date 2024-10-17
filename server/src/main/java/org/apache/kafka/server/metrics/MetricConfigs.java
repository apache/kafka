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
package org.apache.kafka.server.metrics;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Sensor;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class MetricConfigs {
    /** ********* Kafka Metrics Configuration ***********/
    public static final String METRIC_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;
    public static final int METRIC_SAMPLE_WINDOW_MS_DEFAULT = 30000;
    public static final String METRIC_SAMPLE_WINDOW_MS_DOC = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC;

    public static final String METRIC_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;
    public static final int METRIC_NUM_SAMPLES_DEFAULT = 2;
    public static final String METRIC_NUM_SAMPLES_DOC = CommonClientConfigs.METRICS_NUM_SAMPLES_DOC;

    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;
    public static final String METRIC_REPORTER_CLASSES_DEFAULT = JmxReporter.class.getName();
    public static final String METRIC_REPORTER_CLASSES_DOC = CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC;

    public static final String METRIC_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;
    public static final String METRIC_RECORDING_LEVEL_DEFAULT = Sensor.RecordingLevel.INFO.toString();
    public static final String METRIC_RECORDING_LEVEL_DOC = CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC;

    /** ********* Kafka Yammer Metrics Reporters Configuration ***********/
    public static final String KAFKA_METRICS_REPORTER_CLASSES_CONFIG = "kafka.metrics.reporters";
    public static final String KAFKA_METRIC_REPORTER_CLASSES_DEFAULT = "";
    public static final String KAFKA_METRICS_REPORTER_CLASSES_DOC = "A list of classes to use as Yammer metrics custom reporters." +
            " The reporters should implement <code>kafka.metrics.KafkaMetricsReporter</code> trait. If a client wants" +
            " to expose JMX operations on a custom reporter, the custom reporter needs to additionally implement an MBean" +
            " trait that extends <code>kafka.metrics.KafkaMetricsReporterMBean</code> trait so that the registered MBean is compliant with" +
            " the standard MBean convention.";
    public static final String KAFKA_METRICS_POLLING_INTERVAL_SECONDS_CONFIG = "kafka.metrics.polling.interval.secs";
    public static final int KAFKA_METRICS_POLLING_INTERVAL_SECONDS_DEFAULT = 10;
    public static final String KAFKA_METRICS_POLLING_INTERVAL_SECONDS_DOC = "The metrics polling interval (in seconds) which can be used in " +
            KAFKA_METRICS_REPORTER_CLASSES_CONFIG + " implementations.";

    /** ********* Kafka Client Telemetry Metrics Configuration ***********/
    public static final String CLIENT_TELEMETRY_MAX_BYTES_CONFIG = "telemetry.max.bytes";
    public static final int CLIENT_TELEMETRY_MAX_BYTES_DEFAULT = 1024 * 1024;
    public static final String CLIENT_TELEMETRY_MAX_BYTES_DOC = "The maximum size (after compression if compression is used) of" +
            " telemetry metrics pushed from a client to the broker. The default value is 1048576 (1 MB).";

    public static final ConfigDef CONFIG_DEF =  new ConfigDef()
            // Kafka Metrics Configuration
            .define(METRIC_NUM_SAMPLES_CONFIG, INT, METRIC_NUM_SAMPLES_DEFAULT, atLeast(1), LOW, METRIC_NUM_SAMPLES_DOC)
            .define(METRIC_SAMPLE_WINDOW_MS_CONFIG, LONG, METRIC_SAMPLE_WINDOW_MS_DEFAULT, atLeast(1), LOW, METRIC_SAMPLE_WINDOW_MS_DOC)
            .define(METRIC_REPORTER_CLASSES_CONFIG, LIST, METRIC_REPORTER_CLASSES_DEFAULT, LOW, METRIC_REPORTER_CLASSES_DOC)
            .define(METRIC_RECORDING_LEVEL_CONFIG, STRING, METRIC_RECORDING_LEVEL_DEFAULT, LOW, METRIC_RECORDING_LEVEL_DOC)

            // Kafka Yammer Metrics Reporter Configuration
            .define(KAFKA_METRICS_REPORTER_CLASSES_CONFIG, LIST, KAFKA_METRIC_REPORTER_CLASSES_DEFAULT, LOW, KAFKA_METRICS_REPORTER_CLASSES_DOC)
            .define(KAFKA_METRICS_POLLING_INTERVAL_SECONDS_CONFIG, INT, KAFKA_METRICS_POLLING_INTERVAL_SECONDS_DEFAULT, atLeast(1), LOW, KAFKA_METRICS_POLLING_INTERVAL_SECONDS_DOC)

            // Kafka Client Telemetry Metrics Configuration
            .define(CLIENT_TELEMETRY_MAX_BYTES_CONFIG, INT, CLIENT_TELEMETRY_MAX_BYTES_DEFAULT, atLeast(1), LOW, CLIENT_TELEMETRY_MAX_BYTES_DOC);
}

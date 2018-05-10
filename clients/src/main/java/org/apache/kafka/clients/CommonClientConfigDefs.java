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
package org.apache.kafka.clients;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.metrics.Sensor;

import java.util.Collections;

import static org.apache.kafka.common.config.ConfigDef.Importance;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

public class CommonClientConfigDefs {

    public static ConfigKey sendBufferBytes(Integer defaultValue) {
        return new ConfigKey(
                CommonClientConfigs.SEND_BUFFER_CONFIG,
                Type.INT,
                defaultValue,
                atLeast(-1),
                Importance.MEDIUM,
                CommonClientConfigs.SEND_BUFFER_DOC);
    }

    public static ConfigKey receiveBufferBytes(Integer defaultValue) {
        return new ConfigKey(
                CommonClientConfigs.RECEIVE_BUFFER_CONFIG,
                Type.INT,
                defaultValue,
                atLeast(-1),
                Importance.MEDIUM,
                CommonClientConfigs.RECEIVE_BUFFER_DOC);
    }

    public static ConfigKey reconnectBackoffMs(Long defaultValue) {
        return new ConfigKey(
                CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG,
                Type.LONG,
                defaultValue,
                atLeast(0L),
                Importance.LOW,
                CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC);
    }

    public static ConfigKey reconnectBackoffMaxMs(Long defaultValue) {
        return new ConfigKey(
                CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                Type.LONG,
                defaultValue,
                atLeast(0L),
                Importance.LOW,
                CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC);
    }

    public static ConfigKey retryBackoffMs(Long defaultValue) {
        return new ConfigKey(
                CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG,
                Type.LONG,
                defaultValue,
                atLeast(0L),
                Importance.LOW,
                CommonClientConfigs.RETRY_BACKOFF_MS_DOC);
    }

    public static ConfigKey requestTimeoutMs(Integer defaultValue, String doc) {
        return new ConfigKey(
                CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG,
                Type.INT,
                defaultValue,
                atLeast(0),
                Importance.MEDIUM,
                doc);
    }

    public static ConfigKey connectionsMaxIdleMs(Long defaultValue) {
        return new ConfigKey(
                CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG,
                Type.LONG,
                5 * 60 * 1000,
                Importance.MEDIUM,
                CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC);
    }

    public static ConfigKey metadataMaxAge(Long defaultValue) {
        return new ConfigKey(
                CommonClientConfigs.METADATA_MAX_AGE_CONFIG,
                Type.LONG,
                defaultValue,
                atLeast(0),
                Importance.LOW,
                CommonClientConfigs.METADATA_MAX_AGE_DOC);
    }

    public static ConfigKey metricsSampleWindowMs(Long defaultValue) {
        return new ConfigKey(
                CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG,
                Type.LONG,
                defaultValue,
                atLeast(0),
                Importance.LOW,
                CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC);
    }

    public static ConfigKey metricReporterClasses() {
        return new ConfigKey(
                CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
                Type.LIST,
                Collections.emptyList(),
                Importance.LOW,
                CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC);
    }

    public static ConfigKey metricsRecordingLevel() {
        return new ConfigKey(
                CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG,
                Type.STRING,
                Sensor.RecordingLevel.INFO.toString(),
                in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString()),
                Importance.LOW,
                CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC);
    }

    public static ConfigKey metricsNumSamplesConfig(Integer defaultValue) {
        return new ConfigKey(
                CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG,
                Type.INT,
                defaultValue,
                atLeast(1),
                Importance.LOW,
                CommonClientConfigs.METRICS_NUM_SAMPLES_DOC);
    }

    public static ConfigKey bootstrapServers(Object defaultValue, String doc) {
        return new ConfigKey(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                Type.LIST,
                defaultValue,
                new ConfigDef.NonNullValidator(),
                Importance.HIGH,
                doc);
    }

    public static ConfigKey retries(Integer defaultValue, String doc) {
        return new ConfigKey(
                CommonClientConfigs.RETRIES_CONFIG,
                Type.INT,
                defaultValue,
                between(0, Integer.MAX_VALUE), Importance.HIGH,
                doc);
    }

    public static ConfigKey securityProtocol() {
        return new ConfigKey(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                Type.STRING,
                CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                Importance.MEDIUM,
                CommonClientConfigs.SECURITY_PROTOCOL_DOC);
    }

    public static ConfigKey clientId(String doc) {
        return new ConfigKey(
                CommonClientConfigs.CLIENT_ID_CONFIG,
                Type.STRING,
                "",
                Importance.LOW,
                doc);
    }
}

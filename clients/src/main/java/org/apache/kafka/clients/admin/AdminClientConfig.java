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

package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.CommonClientConfigDefs;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Set;

/**
 * The AdminClient configuration class, which also contains constants for configuration entry names.
 */
public class AdminClientConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    /**
     * <code>bootstrap.servers</code>
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /**
     * <code>reconnect.backoff.ms</code>
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /**
     * <code>reconnect.backoff.max.ms</code>
     */
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

    /**
     * <code>retry.backoff.ms</code>
     */
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;
    private static final String RETRY_BACKOFF_MS_DOC = "The amount of time to wait before attempting to " +
                "retry a failed request. This avoids repeatedly sending requests in a tight loop under " +
                "some failure scenarios.";

    /** <code>connections.max.idle.ms</code> */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /** <code>request.timeout.ms</code> */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;

    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;
    private static final String CLIENT_ID_DOC = CommonClientConfigs.CLIENT_ID_DOC;

    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;

    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

    public static final String SECURITY_PROTOCOL_CONFIG = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
    public static final String DEFAULT_SECURITY_PROTOCOL = CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL;

    public static final String RETRIES_CONFIG = CommonClientConfigs.RETRIES_CONFIG;

    static {
        CONFIG = new ConfigDef().define(CommonClientConfigDefs.bootstrapServers(ConfigDef.NO_DEFAULT_VALUE, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC))
                                .define(CommonClientConfigDefs.clientId(CLIENT_ID_DOC))
                                .define(CommonClientConfigDefs.metadataMaxAge(5 * 60 * 1000L))
                                .define(CommonClientConfigDefs.sendBufferBytes(128 * 1024))
                                .define(CommonClientConfigDefs.receiveBufferBytes(64 * 1024))
                                .define(CommonClientConfigDefs.reconnectBackoffMs(50L))
                                .define(CommonClientConfigDefs.reconnectBackoffMaxMs(1000L))
                                .define(CommonClientConfigDefs.retryBackoffMs(100L))
                                .define(CommonClientConfigDefs.requestTimeoutMs(120000, CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC))
                                .define(CommonClientConfigDefs.connectionsMaxIdleMs(5 * 60 * 1000L))

                                .define(CommonClientConfigDefs.retries(5, CommonClientConfigs.RETRIES_DOC))

                                .define(CommonClientConfigDefs.metricsSampleWindowMs(30_000L))
                                .define(CommonClientConfigDefs.metricsNumSamplesConfig(2))
                                .define(CommonClientConfigDefs.metricReporterClasses())
                                .define(CommonClientConfigDefs.metricsRecordingLevel())
                                // security support
                                .define(CommonClientConfigDefs.securityProtocol())
                                .withClientSslSupport()
                                .withClientSaslSupport();
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        return CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
    }

    public AdminClientConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtmlTable());
    }

}

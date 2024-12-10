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
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.kafka.common.utils.Utils.UncheckedCloseable;

/**
 * This class manages the coordination process with brokers for the Connect cluster group membership. It ties together
 * the Coordinator, which implements the group member protocol, with all the other pieces needed to drive the connection
 * to the group coordinator broker. This isolates all the networking to a single thread managed by this class, with
 * higher level operations in response to group membership events being handled by the herder.
 */
public class WorkerGroupMember {
    private static final String JMX_PREFIX = "kafka.connect";

    private final Logger log;
    private final String clientId;
    private final ConsumerNetworkClient client;
    private final Metrics metrics;
    private final WorkerCoordinator coordinator;

    private boolean stopped = false;

    public WorkerGroupMember(DistributedConfig config,
                             String restUrl,
                             ConfigBackingStore configStorage,
                             WorkerRebalanceListener listener,
                             Time time,
                             String clientId,
                             LogContext logContext) {
        try {
            this.clientId = clientId;
            this.log = logContext.logger(WorkerGroupMember.class);

            Map<String, String> metricsTags = new LinkedHashMap<>();
            metricsTags.put("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .tags(metricsTags);
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);

            Map<String, Object> contextLabels = new HashMap<>(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
            contextLabels.put(WorkerConfig.CONNECT_KAFKA_CLUSTER_ID, config.kafkaClusterId());
            contextLabels.put(WorkerConfig.CONNECT_GROUP_ID, config.getString(DistributedConfig.GROUP_ID_CONFIG));
            MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX, contextLabels);

            this.metrics = new Metrics(metricConfig, reporters, time, metricsContext);
            long retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);
            long retryBackoffMaxMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MAX_MS_CONFIG);
            Metadata metadata = new Metadata(retryBackoffMs, retryBackoffMaxMs, config.getLong(CommonClientConfigs.METADATA_MAX_AGE_CONFIG),
                    logContext, new ClusterResourceListeners());
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                    config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
                    config.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG));
            metadata.bootstrap(addresses);
            String metricGrpPrefix = "connect";
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext);
            NetworkClient netClient = new NetworkClient(
                    new Selector(config.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, metricGrpPrefix, channelBuilder, logContext),
                    metadata,
                    clientId,
                    100, // a fixed large enough value will suffice
                    config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    config.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
                    config.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
                    config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                    config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                    config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                    time,
                    true,
                    new ApiVersions(),
                    logContext,
                    config.getLong(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG),
                    MetadataRecoveryStrategy.forName(config.getString(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG))
            );
            this.client = new ConsumerNetworkClient(
                    logContext,
                    netClient,
                    metadata,
                    time,
                    retryBackoffMs,
                    config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                    Integer.MAX_VALUE);
            this.coordinator = new WorkerCoordinator(
                    new GroupRebalanceConfig(config, GroupRebalanceConfig.ProtocolType.CONNECT),
                    logContext,
                    this.client,
                    metrics,
                    metricGrpPrefix,
                    time,
                    restUrl,
                    configStorage,
                    listener,
                    ConnectProtocolCompatibility.compatibility(config.getString(DistributedConfig.CONNECT_PROTOCOL_CONFIG)),
                    config.getInt(DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG));

            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Connect group member created");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed
            // this is to prevent resource leak. see KAFKA-2121
            stop(true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    public void stop() {
        if (stopped) return;
        stop(false);
    }

    /**
     * Ensure that the connection to the broker coordinator is up and that the worker is an
     * active member of the group.
     */
    public void ensureActive(Supplier<UncheckedCloseable> onPoll) {
        coordinator.poll(0, onPoll);
    }

    public void poll(long timeout, Supplier<UncheckedCloseable> onPoll) {
        if (timeout < 0)
            throw new IllegalArgumentException("Timeout must not be negative");
        coordinator.poll(timeout, onPoll);
    }

    /**
     * Interrupt any running poll() calls, causing a WakeupException to be thrown in the thread invoking that method.
     */
    public void wakeup() {
        this.client.wakeup();
    }

    /**
     * Get the member ID of this worker in the group of workers.
     * <p>
     * This ID is the unique member ID automatically generated.
     *
     * @return the member ID
     */
    public String memberId() {
        return coordinator.memberId();
    }

    public void requestRejoin() {
        coordinator.requestRejoin("connect worker requested rejoin");
    }

    public void maybeLeaveGroup(String leaveReason) {
        coordinator.maybeLeaveGroup(leaveReason);
    }

    public String ownerUrl(String connector) {
        return coordinator.ownerUrl(connector);
    }

    public String ownerUrl(ConnectorTaskId task) {
        return coordinator.ownerUrl(task);
    }

    /**
     * Get the version of the connect protocol that is currently active in the group of workers.
     *
     * @return the current connect protocol version
     */
    public short currentProtocolVersion() {
        return coordinator.currentProtocolVersion();
    }

    public void revokeAssignment(ExtendedAssignment assignment) {
        coordinator.revokeAssignment(assignment);
    }

    private void stop(boolean swallowException) {
        log.trace("Stopping the Connect group member.");
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        this.stopped = true;
        Utils.closeQuietly(coordinator, "coordinator", firstException);
        Utils.closeQuietly(metrics, "consumer metrics", firstException);
        Utils.closeQuietly(client, "consumer network client", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        if (firstException.get() != null && !swallowException)
            throw new KafkaException("Failed to stop the Connect group member", firstException.get());
        else
            log.debug("The Connect group member has stopped.");
    }

    // Visible for testing
    Metrics metrics() {
        return this.metrics;
    }
}

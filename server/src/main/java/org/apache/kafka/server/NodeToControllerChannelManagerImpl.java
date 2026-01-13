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

package org.apache.kafka.server;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Manages a communication channel from a node to the active KRaft controller.
 * <p>
 * Creates a network client with the appropriate security configuration and uses a background
 * request thread to queue and send requests asynchronously. Supports dynamic reconfiguration
 * of security settings when the controller listener configuration changes.
 */
public class NodeToControllerChannelManagerImpl implements NodeToControllerChannelManager {
    private static final Logger log = LoggerFactory.getLogger(NodeToControllerChannelManagerImpl.class);
    private final Time time;
    private final Metrics metrics;
    private final AbstractKafkaConfig config;
    private final String channelName;
    private final Long retryTimeoutMs;

    private final LogContext logContext;
    private final ManualMetadataUpdater manualMetadataUpdater = new ManualMetadataUpdater();
    private final ApiVersions apiVersions = new ApiVersions();
    private final NodeToControllerRequestThread requestThread;

    public NodeToControllerChannelManagerImpl(Supplier<ControllerInformation> controllerNodeProvider, Time time, Metrics metrics, AbstractKafkaConfig config, String channelName, String threadNamePrefix, Long retryTimeoutMs) {
        this.time = time;
        this.metrics = metrics;
        this.config = config;
        this.channelName = channelName;
        this.retryTimeoutMs = retryTimeoutMs;
        this.logContext = new LogContext(String.format("[NodeToControllerChannelManager id=%s name=%s] ",
                config.getInt(KRaftConfigs.NODE_ID_CONFIG), channelName));
        String threadName = String.format("%sto-controller-%s-channel-manager", threadNamePrefix, channelName);
        ControllerInformation controllerInformation = controllerNodeProvider.get();
        this.requestThread = new NodeToControllerRequestThread(
                buildNetworkClient(controllerInformation),
                manualMetadataUpdater,
                controllerNodeProvider,
                config,
                time,
                threadName,
                retryTimeoutMs
        );
    }

    private KafkaClient buildNetworkClient(ControllerInformation controllerInfo) {
        ChannelBuilder channelBuilder = ChannelBuilders.clientChannelBuilder(
                controllerInfo.securityProtocol(),
                JaasContext.Type.SERVER,
                config,
                controllerInfo.listenerName(),
                controllerInfo.saslMechanism(),
                time,
                logContext
        );
        if (channelBuilder instanceof Reconfigurable reconfigurable) {
            config.addReconfigurable(reconfigurable);
        }
        Selector selector = new Selector(
                NetworkReceive.UNLIMITED,
                Selector.NO_IDLE_TIMEOUT_MS,
                metrics,
                time,
                channelName,
                Map.of("BrokerId", String.valueOf(config.brokerId())),
                false,
                channelBuilder,
                logContext
        );
        return new NetworkClient(
                selector,
                manualMetadataUpdater,
                String.valueOf(config.brokerId()),
                1,
                50,
                50,
                Selectable.USE_DEFAULT_BUFFER_SIZE,
                Selectable.USE_DEFAULT_BUFFER_SIZE,
                Math.min(Integer.MAX_VALUE, (int) Math.min(config.getInt(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG), retryTimeoutMs)), // request timeout should not exceed the provided retry timeout
                config.getLong(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                config.getLong(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                time,
                true,
                apiVersions,
                logContext,
                MetadataRecoveryStrategy.NONE
        );
    }

    @Override
    public void start() {
        requestThread.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        requestThread.shutdown();
        log.info("Node to controller channel manager for {} shutdown", channelName);
    }

    @Override
    public Optional<NodeApiVersions> controllerApiVersions() {
        return requestThread.activeControllerAddress().flatMap(activeController ->
                Optional.ofNullable(apiVersions.get(activeController.idString()))
        );
    }

    /**
     * Send request to the controller.
     *
     * @param request  The request to be sent.
     * @param callback Request completion callback.
     */
    @Override
    public void sendRequest(AbstractRequest.Builder<? extends AbstractRequest> request,
                            ControllerRequestCompletionHandler callback) {
        requestThread.enqueue(new NodeToControllerQueueItem(
                time.milliseconds(),
                request,
                callback
        ));
    }

    @Override
    public long getTimeoutMs() {
        return retryTimeoutMs;
    }
}

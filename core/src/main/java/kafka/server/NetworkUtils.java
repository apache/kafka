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
package kafka.server;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.Collections;

public class NetworkUtils {

    public static NetworkClient buildNetworkClient(String prefix,
                                                   KafkaConfig config,
                                                   Metrics metrics,
                                                   Time time,
                                                   LogContext logContext) {
        ChannelBuilder channelBuilder = ChannelBuilders.clientChannelBuilder(
            config.interBrokerSecurityProtocol(),
            JaasContext.Type.SERVER,
            config,
            config.interBrokerListenerName(),
            config.saslMechanismInterBrokerProtocol(),
            time,
            config.saslInterBrokerHandshakeRequestEnable(),
            logContext
        );

        if (channelBuilder instanceof Reconfigurable) {
            config.addReconfigurable((Reconfigurable) channelBuilder);
        }

        String metricGroupPrefix = prefix + "-channel";

        Selector selector = new Selector(
            NetworkReceive.UNLIMITED,
            config.connectionsMaxIdleMs(),
            metrics,
            time,
            metricGroupPrefix,
            Collections.emptyMap(),
            false,
            channelBuilder,
            logContext
        );

        String clientId = prefix + "-client-" + config.nodeId();
        return new NetworkClient(
            selector,
            new ManualMetadataUpdater(),
            clientId,
            1,
            50,
            50,
            Selectable.USE_DEFAULT_BUFFER_SIZE,
            config.socketReceiveBufferBytes(),
            config.requestTimeoutMs(),
            config.connectionSetupTimeoutMs(),
            config.connectionSetupTimeoutMaxMs(),
            time,
            true,
            new ApiVersions(),
            logContext
        );
    }
}
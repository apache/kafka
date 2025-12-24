/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.kafka.server;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.raft.RaftManager;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.config.AbstractKafkaConfig;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * Finds the controller node by checking the metadata log manager.
 * This provider is used when we are using a Raft-based metadata quorum.
 */
public class RaftControllerNodeProvider implements ControllerNodeProvider {

    private final RaftManager<ApiMessageAndVersion> raftManager;
    private final ListenerName listenerName;
    private final SecurityProtocol securityProtocol;
    private final String saslMechanism;

    public static RaftControllerNodeProvider create(RaftManager<ApiMessageAndVersion> raftManager, AbstractKafkaConfig config) {
        final ListenerName controllerListenerName = new ListenerName(config.controllerListenerNames().getFirst());
        final SecurityProtocol controllerSecurityProtocol = Optional.of(config.effectiveListenerSecurityProtocolMap().get(controllerListenerName))
                .orElse(SecurityProtocol.forName(controllerListenerName.value()));
        final String controllerSaslMechanism = config.getString(KRaftConfigs.SASL_MECHANISM_CONTROLLER_PROTOCOL_CONFIG);
        return new RaftControllerNodeProvider(raftManager, controllerListenerName, controllerSecurityProtocol, controllerSaslMechanism);
    }

    public RaftControllerNodeProvider(RaftManager<ApiMessageAndVersion> raftManager, ListenerName listenerName, SecurityProtocol securityProtocol, String saslMechanism) {
        this.raftManager = raftManager;
        this.listenerName = listenerName;
        this.securityProtocol = securityProtocol;
        this.saslMechanism = saslMechanism;
    }

    @SuppressWarnings("resource")  // RaftClient lifecycle managed by RaftManager
    private Optional<Node> idToNode(int id) {
        return raftManager.client().voterNode(id, listenerName);
    }

    @SuppressWarnings("resource")
    @Override
    public ControllerInformation getControllerInfo() {
        OptionalInt leaderIdOpt = raftManager.client().leaderAndEpoch().leaderId();

        Optional<Node> node = leaderIdOpt.isPresent()
                ? idToNode(leaderIdOpt.getAsInt())
                : Optional.empty();

        return new ControllerInformation(node, listenerName, securityProtocol, saslMechanism);
    }
}

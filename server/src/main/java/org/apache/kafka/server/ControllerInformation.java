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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Optional;

/**
 * Connection information for communicating with the active Kafka controller in KRaft mode.
 * <p>
 * Contains the controller node endpoint and security configuration needed to establish
 * a network connection. The node may be absent during cluster initialization or leader elections.
 *
 * @param node The controller node (id, host, port), or empty if controller is unknown
 * @param listenerName The listener to use for controller connections
 * @param securityProtocol The security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
 * @param saslMechanism The SASL mechanism for authentication (e.g., "PLAIN", "SCRAM-SHA-256")
 */
public record ControllerInformation(Optional<Node> node, ListenerName listenerName, SecurityProtocol securityProtocol,
                                    String saslMechanism) {
}

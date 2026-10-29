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
package org.apache.kafka.network;

import org.apache.kafka.common.utils.Utils;

import java.nio.channels.SocketChannel;
import java.util.Set;

public class SocketServer {

    public static final String METRICS_GROUP = "socket-server-metrics";

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
            SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG,
            SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG,
            SocketServerConfigs.MAX_CONNECTIONS_CONFIG,
            SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG);

    public static final Set<String> LISTENER_RECONFIGURABLE_CONFIGS = Set.of(
            SocketServerConfigs.MAX_CONNECTIONS_CONFIG,
            SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG);

    public static void closeSocket(SocketChannel channel) {
        Utils.closeQuietly(channel.socket(), "channel socket");
        Utils.closeQuietly(channel, "channel");
    }
}

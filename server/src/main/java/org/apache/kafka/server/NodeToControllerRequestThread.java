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
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import java.util.Collection;
import java.util.List;

public class NodeToControllerRequestThread extends InterBrokerSendThread {

    public NodeToControllerRequestThread(KafkaClient initialNetworkClient,
                                         ManualMetadataUpdater metadataUpdater,
                                         ControllerNodeProvider controllerNodeProvider,
                                         AbstractKafkaConfig config,
                                         Time time,
                                         String threadName,
                                         Long retryTimeoutMs) {
        super(threadName, initialNetworkClient, Math.min(Integer.MAX_VALUE, (int) Math.min(config.getLong(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG), retryTimeoutMs)), time, false);
    }

    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        return List.of();
    }
}

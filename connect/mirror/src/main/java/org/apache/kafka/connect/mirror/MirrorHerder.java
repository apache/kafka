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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;

import java.util.List;

public class MirrorHerder extends DistributedHerder {

    private final Runnable onLeader;

    public MirrorHerder(Runnable onLeader, DistributedConfig config, Time time, Worker worker, String kafkaClusterId, StatusBackingStore statusBackingStore, ConfigBackingStore configBackingStore, String restUrl, RestClient restClient, ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy, List<String> restNamespace, AutoCloseable... uponShutdown) {
        super(config, time, worker, kafkaClusterId, statusBackingStore, configBackingStore, restUrl, restClient, connectorClientConfigOverridePolicy, restNamespace, uponShutdown);
        this.onLeader = onLeader;
    }

    @Override
    protected boolean handleRebalanceCompleted() {
        if (!super.handleRebalanceCompleted()) {
            return false;
        }
        if (isLeader()) {
            onLeader.run();
        }
        return true;
    }
}

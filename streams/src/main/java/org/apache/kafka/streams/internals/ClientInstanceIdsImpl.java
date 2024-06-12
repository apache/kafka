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
package org.apache.kafka.streams.internals;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.ClientInstanceIds;

import java.util.HashMap;
import java.util.Map;

public class ClientInstanceIdsImpl implements ClientInstanceIds {
    private final Map<String, Uuid> consumerInstanceIds = new HashMap<>();
    private final Map<String, Uuid> producerInstanceIds = new HashMap<>();
    private Uuid adminInstanceId;

    public void addConsumerInstanceId(final String key, final Uuid instanceId) {
        consumerInstanceIds.put(key, instanceId);
    }

    public void addProducerInstanceId(final String key, final Uuid instanceId) {
        producerInstanceIds.put(key, instanceId);
    }

    public void setAdminInstanceId(final Uuid instanceId) {
        adminInstanceId = instanceId;
    }

    @Override
    public Uuid adminInstanceId() {
        if (adminInstanceId == null) {
            throw new IllegalStateException(
                "Telemetry is not enabled on the admin client." +
                    " Set config `" + AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");
        }
        return adminInstanceId;
    }

    @Override
    public Map<String, Uuid> consumerInstanceIds() {
        return consumerInstanceIds;
    }

    @Override
    public Map<String, Uuid> producerInstanceIds() {
        return producerInstanceIds;
    }
}

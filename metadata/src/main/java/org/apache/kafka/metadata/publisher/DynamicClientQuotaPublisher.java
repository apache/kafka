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
package org.apache.kafka.metadata.publisher;

import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.image.ClientQuotaDelta;
import org.apache.kafka.image.ClientQuotasDelta;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.fault.FaultHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.IP;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;

/**
 * Publishes dynamic client quota changes to quota managers.
 */
public class DynamicClientQuotaPublisher implements MetadataPublisher {
    /**
     * Updates quotas for users, client IDs, or their combinations.
     * Null values in the entity entries represent default quota entities.
     * An empty quota value removes the quota override for the entity.
     */
    @FunctionalInterface
    public interface UserClientQuotaUpdater {
        void updateQuota(ClientQuotaEntity entity, OptionalDouble newValue);
    }

    /**
     * Updates IP connection rate quotas.
     * An empty address represents the default IP entity.
     * An empty quota value removes the quota override for the entity.
     */
    @FunctionalInterface
    public interface IpQuotaUpdater {
        void updateQuota(Optional<InetAddress> inetAddress, OptionalInt newValue);
    }

    private static final Logger log = LoggerFactory.getLogger(DynamicClientQuotaPublisher.class);

    private final int nodeId;
    private final FaultHandler faultHandler;
    private final String nodeType;
    private final Map<String, UserClientQuotaUpdater> userClientQuotaUpdaters;
    private final IpQuotaUpdater ipQuotaUpdater;

    public DynamicClientQuotaPublisher(
        int nodeId,
        FaultHandler faultHandler,
        String nodeType,
        Map<String, UserClientQuotaUpdater> userClientQuotaUpdaters,
        IpQuotaUpdater ipQuotaUpdater
    ) {
        this.nodeId = nodeId;
        this.faultHandler = faultHandler;
        this.nodeType = nodeType;
        this.userClientQuotaUpdaters = userClientQuotaUpdaters;
        this.ipQuotaUpdater = ipQuotaUpdater;
    }

    @Override
    public String name() {
        return "DynamicClientQuotaPublisher " + nodeType + " id=" + nodeId;
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        ClientQuotasDelta clientQuotasDelta = delta.clientQuotasDelta();
        if (clientQuotasDelta != null) {
            try {
                clientQuotasDelta.changes().forEach(this::update);
            } catch (Throwable t) {
                faultHandler.handleFault("Uncaught exception while publishing dynamic client quota changes from MetadataDelta up to " + newImage.highestOffsetAndEpoch().offset(), t);
            }
        }
    }

    private void update(ClientQuotaEntity entity, ClientQuotaDelta quotaDelta) {
        Map<String, String> entries = entity.entries();
        if (entries.containsKey(IP)) {
            handleIpQuota(entity, quotaDelta);
        } else if (entries.containsKey(USER) || entries.containsKey(CLIENT_ID)) {
            handleUserClientQuota(entity, quotaDelta);
        } else {
            log.warn("Ignoring unsupported quota entity {}.", entity);
        }
    }

    private void handleIpQuota(ClientQuotaEntity entity, ClientQuotaDelta quotaDelta) {
        quotaDelta.changes().forEach((key, value) -> {
            // The connection quota only understands the connection rate limit.
            if (!key.equals(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG)) {
                log.warn("Ignoring unexpected quota key {} for entity {}", key, entity);
                return;
            }

            // An empty Optional represents the default IP entity.
            Optional<InetAddress> inetAddress = Optional.ofNullable(entity.entries().get(IP)).map(ip -> {
                try {
                    return InetAddress.getByName(ip);
                } catch (UnknownHostException e) {
                    throw new IllegalArgumentException("Unable to resolve address " + ip, e);
                }
            });

            try {
                ipQuotaUpdater.updateQuota(inetAddress, value.isPresent() ? OptionalInt.of((int) value.getAsDouble()) : OptionalInt.empty());
            } catch (Throwable t) {
                log.error("Failed to update IP quota {}", entity, t);
            }
        });
    }

    private void handleUserClientQuota(ClientQuotaEntity entity, ClientQuotaDelta quotaDelta) {
        quotaDelta.changes().forEach((key, value) -> {
            UserClientQuotaUpdater userClientQuotaUpdater = userClientQuotaUpdaters.get(key);
            if (userClientQuotaUpdater == null) {
                log.warn("Ignoring unexpected quota key {} for entity {}", key, entity);
                return;
            }

            try {
                userClientQuotaUpdater.updateQuota(entity, value);
            } catch (Throwable t) {
                log.error("Failed to update user-client quota {}", entity, t);
            }
        });
    }
}

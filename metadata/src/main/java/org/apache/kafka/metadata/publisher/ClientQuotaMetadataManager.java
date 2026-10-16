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
import org.apache.kafka.metadata.publisher.QuotaEntity.IpQuotaEntity;
import org.apache.kafka.metadata.publisher.QuotaEntity.UserClientQuotaEntity;
import org.apache.kafka.server.config.QuotaConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Processes quota metadata records as they appear in the metadata log and updates quota managers
 * and cache as necessary.
 */
public class ClientQuotaMetadataManager implements Consumer<ClientQuotasDelta> {
    private static final Logger log = LoggerFactory.getLogger(ClientQuotaMetadataManager.class);

    private final Map<String, BiConsumer<UserClientQuotaEntity, OptionalDouble>> userClientQuotaUpdaters;
    private final BiConsumer<Optional<InetAddress>, OptionalInt> ipQuotaUpdater;

    public ClientQuotaMetadataManager(
        Map<String, BiConsumer<UserClientQuotaEntity, OptionalDouble>> userClientQuotaUpdaters,
        BiConsumer<Optional<InetAddress>, OptionalInt> ipQuotaUpdater
    ) {
        this.userClientQuotaUpdaters = userClientQuotaUpdaters;
        this.ipQuotaUpdater = ipQuotaUpdater;
    }

    @Override
    public void accept(ClientQuotasDelta quotasDelta) {
        quotasDelta.changes().forEach(this::update);
    }

    private void update(ClientQuotaEntity entity, ClientQuotaDelta quotaDelta) {
        QuotaEntity.fromClientQuotaEntity(entity).ifPresentOrElse(quotaEntity -> {
            if (quotaEntity instanceof IpQuotaEntity ipEntity) {
                handleIpQuota(ipEntity, quotaDelta);
            } else if (quotaEntity instanceof UserClientQuotaEntity userClientEntity) {
                handleUserClientQuota(userClientEntity, quotaDelta);
            }
        }, () -> log.warn("Ignoring unsupported quota entity {}.", entity));
    }

    private void handleIpQuota(IpQuotaEntity ipEntity, ClientQuotaDelta quotaDelta) {
        // An empty Optional represents the default IP entity.
        Optional<InetAddress> inetAddress = ipEntity.ipAddress().map(ip -> {
            try {
                return InetAddress.getByName(ip);
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Unable to resolve address " + ip);
            }
        });

        quotaDelta.changes().forEach((key, value) -> {
            // The connection quota only understands the connection rate limit.
            if (!key.equals(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG)) {
                log.warn("Ignoring unexpected quota key {} for entity {}", key, ipEntity);
                return;
            }
            try {
                ipQuotaUpdater.accept(inetAddress, value.isPresent() ? OptionalInt.of((int) value.getAsDouble()) : OptionalInt.empty());
            } catch (Throwable t) {
                log.error("Failed to update IP quota {}", ipEntity, t);
            }
        });
    }

    private void handleUserClientQuota(UserClientQuotaEntity entity, ClientQuotaDelta quotaDelta) {
        quotaDelta.changes().forEach((key, value) -> {
            BiConsumer<UserClientQuotaEntity, OptionalDouble> userClientQuotaUpdater = userClientQuotaUpdaters.get(key);
            if (userClientQuotaUpdater == null) {
                log.warn("Ignoring unexpected quota key {} for entity {}", key, entity);
                return;
            }

            try {
                userClientQuotaUpdater.accept(entity, value);
            } catch (Throwable t) {
                log.error("Failed to update user-client quota {}", entity, t);
            }
        });
    }
}

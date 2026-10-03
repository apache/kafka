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

import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.IP;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;

/**
 * Quota entities with names as stored in metadata.
 */
public sealed interface QuotaEntity {
    /**
     * An explicit or default IP quota entity.
     */
    sealed interface IpQuotaEntity extends QuotaEntity {
        default Optional<String> ipAddress() {
            return Optional.empty();
        }
    }

    /**
     * A quota entity for a user, a client ID, or both.
     */
    sealed interface UserClientQuotaEntity extends QuotaEntity {
        default Optional<UserQuotaEntity> userEntity() {
            return Optional.empty();
        }

        default Optional<ClientIdQuotaEntity> clientIdEntity() {
            return Optional.empty();
        }
    }

    /**
     * An explicit or default user dimension.
     */
    sealed interface UserQuotaEntity extends UserClientQuotaEntity {
        @Override
        default Optional<UserQuotaEntity> userEntity() {
            return Optional.of(this);
        }
    }

    /**
     * An explicit or default client ID dimension.
     */
    sealed interface ClientIdQuotaEntity extends UserClientQuotaEntity {
        @Override
        default Optional<ClientIdQuotaEntity> clientIdEntity() {
            return Optional.of(this);
        }
    }

    /**
     * An explicit IP address.
     */
    record IpEntity(String ip) implements IpQuotaEntity {
        @Override
        public Optional<String> ipAddress() {
            return Optional.of(ip);
        }
    }

    /**
     * The default IP entity.
     */
    record DefaultIpEntity() implements IpQuotaEntity { }

    /**
     * An explicit user with no client ID dimension.
     */
    record UserEntity(String user) implements UserQuotaEntity { }

    /**
     * The default user with no client ID dimension.
     */
    record DefaultUserEntity() implements UserQuotaEntity { }

    /**
     * An explicit client ID with no user dimension.
     */
    record ClientIdEntity(String clientId) implements ClientIdQuotaEntity { }

    /**
     * The default client ID with no user dimension.
     */
    record DefaultClientIdEntity() implements ClientIdQuotaEntity { }

    /**
     * An explicit user and an explicit client ID.
     */
    record ExplicitUserExplicitClientIdEntity(String user, String clientId) implements UserClientQuotaEntity {
        @Override
        public Optional<UserQuotaEntity> userEntity() {
            return Optional.of(new UserEntity(user));
        }

        @Override
        public Optional<ClientIdQuotaEntity> clientIdEntity() {
            return Optional.of(new ClientIdEntity(clientId));
        }
    }

    /**
     * An explicit user and the default client ID.
     */
    record ExplicitUserDefaultClientIdEntity(String user) implements UserClientQuotaEntity {
        @Override
        public Optional<UserQuotaEntity> userEntity() {
            return Optional.of(new UserEntity(user));
        }

        @Override
        public Optional<ClientIdQuotaEntity> clientIdEntity() {
            return Optional.of(new DefaultClientIdEntity());
        }
    }

    /**
     * The default user and an explicit client ID.
     */
    record DefaultUserExplicitClientIdEntity(String clientId) implements UserClientQuotaEntity {
        @Override
        public Optional<UserQuotaEntity> userEntity() {
            return Optional.of(new DefaultUserEntity());
        }

        @Override
        public Optional<ClientIdQuotaEntity> clientIdEntity() {
            return Optional.of(new ClientIdEntity(clientId));
        }
    }

    /**
     * The default user and default client ID.
     */
    record DefaultUserDefaultClientIdEntity() implements UserClientQuotaEntity {
        @Override
        public Optional<UserQuotaEntity> userEntity() {
            return Optional.of(new DefaultUserEntity());
        }

        @Override
        public Optional<ClientIdQuotaEntity> clientIdEntity() {
            return Optional.of(new DefaultClientIdEntity());
        }
    }

    /**
     * Creates a user-only quota entity. A null name denotes the default user.
     */
    private static UserQuotaEntity userQuotaEntity(String user) {
        return user == null ? new DefaultUserEntity() : new UserEntity(user);
    }

    /**
     * Creates a client-ID-only quota entity. A null name denotes the default client ID.
     */
    private static ClientIdQuotaEntity clientIdQuotaEntity(String clientId) {
        return clientId == null ? new DefaultClientIdEntity() : new ClientIdEntity(clientId);
    }

    /**
     * Creates a user + client ID quota entity. A null name in either dimension denotes its default.
     */
    private static UserClientQuotaEntity userClientQuotaEntity(String user, String clientId) {
        if (user == null && clientId == null) {
            return new DefaultUserDefaultClientIdEntity();
        }
        if (user == null) {
            return new DefaultUserExplicitClientIdEntity(clientId);
        }
        if (clientId == null) {
            return new ExplicitUserDefaultClientIdEntity(user);
        }
        return new ExplicitUserExplicitClientIdEntity(user, clientId);
    }

    /**
     * Parses a quota entity, returning empty if unsupported.
     */
    static Optional<QuotaEntity> fromClientQuotaEntity(ClientQuotaEntity entity) {
        Map<String, String> entries = entity.entries();
        if (entries.containsKey(IP)) {
            // A null IP address denotes the default entity.
            String ip = entries.get(IP);
            return Optional.of(ip == null ? new DefaultIpEntity() : new IpEntity(ip));
        }

        // Metadata uses null for default entities, so use containsKey to distinguish them from absent dimensions.
        boolean hasUser = entries.containsKey(USER);
        boolean hasClientId = entries.containsKey(CLIENT_ID);
        String user = entries.get(USER);
        String clientId = entries.get(CLIENT_ID);

        if (hasUser && hasClientId) {
            return Optional.of(userClientQuotaEntity(user, clientId));
        }
        if (hasUser) {
            return Optional.of(userQuotaEntity(user));
        }
        if (hasClientId) {
            return Optional.of(clientIdQuotaEntity(clientId));
        }
        return Optional.empty();
    }
}

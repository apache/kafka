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

package org.apache.kafka.image;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData.EntityData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData.EntryData;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.image.node.ClientQuotasImageNode;
import org.apache.kafka.image.writer.ImageWriter;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.IP;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;
import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_DEFAULT;
import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_EXACT;
import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_SPECIFIED;

/**
 * Represents the client quotas in the metadata image.
 * <p>
 * This class is thread-safe.
 */
public class ClientQuotasImage {
    public static final ClientQuotasImage EMPTY = new ClientQuotasImage(Map.of());

    private final Map<ClientQuotaEntity, ClientQuotaImage> entities;
    private final Map<String, Map<String, Set<ClientQuotaEntity>>> index;

    public ClientQuotasImage(Map<ClientQuotaEntity, ClientQuotaImage> entities) {
        this.entities = Collections.unmodifiableMap(entities);
        this.index = new HashMap<>();
        for (ClientQuotaEntity entity : this.entities.keySet()) {
            for (Entry<String, String> entry : entity.entries().entrySet()) {
                index.computeIfAbsent(entry.getKey(), k -> new HashMap<>())
                        .computeIfAbsent(entry.getValue(), k -> new HashSet<>())
                        .add(entity);
            }
        }
    }

    public Map<ClientQuotaEntity, ClientQuotaImage> entities() {
        return entities;
    }

    public boolean isEmpty() {
        return entities.isEmpty();
    }

    public void write(ImageWriter writer) {
        for (Entry<ClientQuotaEntity, ClientQuotaImage> entry : entities.entrySet()) {
            ClientQuotaEntity entity = entry.getKey();
            ClientQuotaImage clientQuotaImage = entry.getValue();
            clientQuotaImage.write(entity, writer);
        }
    }

    public DescribeClientQuotasResponseData describe(DescribeClientQuotasRequestData request) {
        DescribeClientQuotasResponseData response = new DescribeClientQuotasResponseData();
        Map<String, String> exactMatch = new HashMap<>();
        Set<String> typeMatch = new HashSet<>();
        for (DescribeClientQuotasRequestData.ComponentData component : request.components()) {
            if (component.entityType().isEmpty()) {
                throw new InvalidRequestException("Invalid empty entity type.");
            } else if (exactMatch.containsKey(component.entityType()) ||
                typeMatch.contains(component.entityType())) {
                throw new InvalidRequestException("Entity type " + component.entityType() +
                    " cannot appear more than once in the filter.");
            }
            if (!(component.entityType().equals(IP) || component.entityType().equals(USER) ||
                component.entityType().equals(CLIENT_ID))) {
                throw new UnsupportedVersionException("Unsupported entity type " +
                    component.entityType());
            }
            switch (component.matchType()) {
                case MATCH_TYPE_EXACT:
                    if (component.match() == null) {
                        throw new InvalidRequestException("Request specified " +
                            "MATCH_TYPE_EXACT, but set match string to null.");
                    }
                    exactMatch.put(component.entityType(), component.match());
                    break;
                case MATCH_TYPE_DEFAULT:
                    if (component.match() != null) {
                        throw new InvalidRequestException("Request specified " +
                            "MATCH_TYPE_DEFAULT, but also specified a match string.");
                    }
                    exactMatch.put(component.entityType(), null);
                    break;
                case MATCH_TYPE_SPECIFIED:
                    if (component.match() != null) {
                        throw new InvalidRequestException("Request specified " +
                            "MATCH_TYPE_SPECIFIED, but also specified a match string.");
                    }
                    typeMatch.add(component.entityType());
                    break;
                default:
                    throw new InvalidRequestException("Unknown match type " + component.matchType());
            }
        }
        if (exactMatch.containsKey(IP) || typeMatch.contains(IP)) {
            if ((exactMatch.containsKey(USER) || typeMatch.contains(USER)) ||
                    (exactMatch.containsKey(CLIENT_ID) || typeMatch.contains(CLIENT_ID))) {
                throw new InvalidRequestException("Invalid entity filter component " +
                    "combination. IP filter component should not be used with " +
                    "user or clientId filter component.");
            }
        }
        Set<ClientQuotaEntity> candidates = null;

        // 1. Filter by exact matches
        for (Entry<String, String> entry : exactMatch.entrySet()) {
            Map<String, Set<ClientQuotaEntity>> nameMap = index.get(entry.getKey());
            Set<ClientQuotaEntity> matches = (nameMap == null) ? Collections.emptySet()
                    : nameMap.getOrDefault(entry.getValue(), Collections.emptySet());

            if (matches.isEmpty()) {
                return response;
            }

            if (candidates == null) {
                candidates = new HashSet<>(matches);
            } else {
                candidates.retainAll(matches);
            }

            if (candidates.isEmpty()) {
                return response;
            }
        }

        // 2. Filter by type matches
        for (String type : typeMatch) {
            Map<String, Set<ClientQuotaEntity>> nameMap = index.get(type);
            Set<ClientQuotaEntity> matches = new HashSet<>();
            if (nameMap != null) {
                for (Set<ClientQuotaEntity> s : nameMap.values()) {
                    matches.addAll(s);
                }
            }

            if (matches.isEmpty()) {
                return response;
            }

            if (candidates == null) {
                candidates = new HashSet<>(matches);
            } else {
                candidates.retainAll(matches);
            }

            if (candidates.isEmpty()) {
                return response;
            }
        }

        if (candidates == null) {
            for (Entry<ClientQuotaEntity, ClientQuotaImage> entry : entities.entrySet()) {
                ClientQuotaEntity entity = entry.getKey();
                ClientQuotaImage quotaImage = entry.getValue();
                if (matches(entity, exactMatch, typeMatch, request.strict())) {
                    response.entries().add(toDescribeEntry(entity, quotaImage));
                }
            }
        } else {
            for (ClientQuotaEntity entity : candidates) {
                ClientQuotaImage quotaImage = entities.get(entity);
                if (matches(entity, exactMatch, typeMatch, request.strict())) {
                    response.entries().add(toDescribeEntry(entity, quotaImage));
                }
            }
        }
        return response;
    }

    private static boolean matches(ClientQuotaEntity entity,
                                   Map<String, String> exactMatch,
                                   Set<String> typeMatch,
                                   boolean strict) {
        if (strict) {
            if (entity.entries().size() != exactMatch.size() + typeMatch.size()) {
                return false;
            }
        }
        for (Entry<String, String> entry : exactMatch.entrySet()) {
            if (!entity.entries().containsKey(entry.getKey())) {
                return false;
            }
            if (!Objects.equals(entity.entries().get(entry.getKey()), entry.getValue())) {
                return false;
            }
        }
        for (String type : typeMatch) {
            if (!entity.entries().containsKey(type)) {
                return false;
            }
        }
        return true;
    }

    private static EntryData toDescribeEntry(ClientQuotaEntity entity,
                                             ClientQuotaImage quotaImage) {
        EntryData data = new EntryData();
        for (Entry<String, String> entry : entity.entries().entrySet()) {
            data.entity().add(new EntityData().
                setEntityType(entry.getKey()).
                setEntityName(entry.getValue()));
        }
        data.setValues(quotaImage.toDescribeValues());
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ClientQuotasImage that = (ClientQuotasImage) o;
        return Objects.equals(entities, that.entities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entities);
    }

    @Override
    public String toString() {
        return new ClientQuotasImageNode(this).stringify();
    }
}
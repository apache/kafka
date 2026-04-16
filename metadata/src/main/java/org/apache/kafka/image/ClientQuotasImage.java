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
public final class ClientQuotasImage {
    public static final ClientQuotasImage EMPTY = new ClientQuotasImage(Map.of());

    private final Map<ClientQuotaEntity, ClientQuotaImage> entities;

    // Map from entity type to entity name to set of entries. The entity type could be "user", "client-id", and "ip".
    // {
    //   "user": { "user1": {entity1: image1}, "user2": {entity2: image2} },
    //   "client-id": { "client-id1": {entity3: image3}, "client-id2": {entity4: image4} },
    //   "ip": { "ip1": {entity5: image5}, "ip2": {entity6: image6} }
    // }
    private final Map<String, Map<String, Map<ClientQuotaEntity, ClientQuotaImage>>> entitiesByTypeAndName;

    // Map from entity type to set of entries. The entity type could be "user", "client-id", and "ip".
    // {
    //   "user": { entity1: image1, entity2: image2 },
    //   "client-id": { entity3: image3, entity4: image4 },
    //   "ip": { entity5: image5, entity6: image6 }
    // }
    private final Map<String, Map<ClientQuotaEntity, ClientQuotaImage>> entitiesByType;

    public ClientQuotasImage(Map<ClientQuotaEntity, ClientQuotaImage> entities) {
        this.entities = Collections.unmodifiableMap(entities);
        var entitiesByTypeAndName = new HashMap<String, Map<String, Map<ClientQuotaEntity, ClientQuotaImage>>>();
        var entitiesByType = new HashMap<String, Map<ClientQuotaEntity, ClientQuotaImage>>();
        for (var entry : entities.entrySet()) {
            ClientQuotaEntity entity = entry.getKey();
            for (var entityEntry : entity.entries().entrySet()) {
                entitiesByTypeAndName
                    .computeIfAbsent(entityEntry.getKey(), k -> new HashMap<>())
                    .computeIfAbsent(entityEntry.getValue(), k -> new HashMap<>())
                    .put(entity, entry.getValue());

                entitiesByType
                    .computeIfAbsent(entityEntry.getKey(), k -> new HashMap<>())
                    .put(entity, entry.getValue());
            }
        }
        this.entitiesByTypeAndName = Collections.unmodifiableMap(entitiesByTypeAndName);
        this.entitiesByType = Collections.unmodifiableMap(entitiesByType);
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

        return matches(exactMatch, typeMatch, request.strict());
    }

    private DescribeClientQuotasResponseData matches(
        Map<String, String> exactMatch,
        Set<String> typeMatch,
        boolean strict
    ) {
        DescribeClientQuotasResponseData response = new DescribeClientQuotasResponseData();
        Map<ClientQuotaEntity, ClientQuotaImage> candidates = null;
        // Case 1: exact match exists. Filter candidates based on exact match first and then type match
        if (!exactMatch.isEmpty()) {
            for (Entry<String, String> exactMatchEntry : exactMatch.entrySet()) {
                String entityType = exactMatchEntry.getKey();
                String entityName = exactMatchEntry.getValue();
                var nameMap = entitiesByTypeAndName.get(entityType);
                var matches = Map.<ClientQuotaEntity, ClientQuotaImage>of();
                if (nameMap != null) matches = nameMap.getOrDefault(entityName, Map.of());
                if (candidates == null) {
                    candidates = new HashMap<>(matches);
                } else {
                    candidates.keySet().retainAll(matches.keySet());
                }
            }

            for (String type : typeMatch) {
                candidates.keySet().retainAll(entitiesByType.getOrDefault(type, Map.of()).keySet());
            }
        } else if (!typeMatch.isEmpty()) {
            // Case 2: no exact match, only type match exists
            for (String type : typeMatch) {
                Map<ClientQuotaEntity, ClientQuotaImage> matches = entitiesByType.getOrDefault(type, Map.of());
                if (candidates == null) {
                    candidates = new HashMap<>(matches);
                } else {
                    candidates.keySet().retainAll(matches.keySet());
                }
            }
        } else if (!strict) {
            // Case 3: no exact match, no type match, no strict, return all entries
            for (Entry<ClientQuotaEntity, ClientQuotaImage> entry : entities.entrySet()) {
                response.entries().add(toDescribeEntry(entry.getKey(), entry.getValue()));
            }
            return response;
        }

        if (candidates != null) {
            for (Entry<ClientQuotaEntity, ClientQuotaImage> entry : candidates.entrySet()) {
                if (!strict || entry.getKey().entries().size() == exactMatch.size() + typeMatch.size()) {
                    response.entries().add(toDescribeEntry(entry.getKey(), entry.getValue()));
                }
            }
        }
        return response;
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
        if (this == o) return true;
        if (!(o instanceof ClientQuotasImage other)) return false;
        return entities.equals(other.entities);
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

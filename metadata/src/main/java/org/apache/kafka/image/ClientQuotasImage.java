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
    //   "ip": { "ip1": {entyty5: image5}, "ip2": {entyty6: image6} }
    // }
    private final Map<String, Map<String, Map<ClientQuotaEntity, ClientQuotaImage>>> entitiesByType;

    public ClientQuotasImage(Map<ClientQuotaEntity, ClientQuotaImage> entities) {
        this.entities = Collections.unmodifiableMap(entities);
        Map<String, Map<String, Map<ClientQuotaEntity, ClientQuotaImage>>> entitiesByType = new HashMap<>();
        for (Entry<ClientQuotaEntity, ClientQuotaImage> entry : entities.entrySet()) {
            ClientQuotaEntity entity = entry.getKey();
            for (Entry<String, String> entityEntry : entity.entries().entrySet()) {
                entitiesByType
                    .computeIfAbsent(entityEntry.getKey(), k -> new HashMap<>())
                    .computeIfAbsent(entityEntry.getValue(), k -> new HashMap<>())
                    .putIfAbsent(entity, entry.getValue());
            }
        }
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

        Set<ClientQuotaEntity> addedEntities = new HashSet<>();
        for (Entry<String, String> exactMatchEntry : exactMatch.entrySet()) {
            String entityType = exactMatchEntry.getKey();
            String entityName = exactMatchEntry.getValue();
            for (Entry<ClientQuotaEntity, ClientQuotaImage> entry : entitiesByType.getOrDefault(entityType, Map.of()).getOrDefault(entityName, Map.of()).entrySet()) {
                if (request.strict() && !entry.getKey().entries().equals(exactMatch)) {
                    continue;
                }
                addEntryToResponse(response, addedEntities, entry, exactMatch.size(), typeMatch.size(), request.strict());
            }
        }

        for (String type : typeMatch) {
            for (Map<ClientQuotaEntity, ClientQuotaImage> entityToImage : entitiesByType.getOrDefault(type, Map.of()).values()) {
                for (Entry<ClientQuotaEntity, ClientQuotaImage> entry : entityToImage.entrySet()) {
                    addEntryToResponse(response, addedEntities, entry, typeMatch.size(), exactMatch.size(), request.strict());
                }
            }
        }

        if (!request.strict() && exactMatch.isEmpty() && typeMatch.isEmpty()) {
            for (Entry<ClientQuotaEntity, ClientQuotaImage> entry : entities.entrySet()) {
                addEntryToResponse(response, addedEntities, entry, 0, 0, false);
            }
        }

        return response;
    }

    private void addEntryToResponse(
        DescribeClientQuotasResponseData response,
        Set<ClientQuotaEntity> addedEntities,
        Entry<ClientQuotaEntity, ClientQuotaImage> entry,
        int exactMatchSize,
        int typeMatchSize,
        boolean strict
    ) {
        ClientQuotaEntity entity = entry.getKey();
        ClientQuotaImage clientQuotaImage = entry.getValue();
        if (strict && entity.entries().size() != exactMatchSize + typeMatchSize) {
            return;
        }
        if (!addedEntities.contains(entity)) {
            addedEntities.add(entity);
            response.entries().add(toDescribeEntry(entity, clientQuotaImage));
        }
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

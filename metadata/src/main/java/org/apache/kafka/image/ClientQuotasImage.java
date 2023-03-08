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
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.IP;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;
import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_EXACT;
import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_DEFAULT;
import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_SPECIFIED;


/**
 * Represents the client quotas in the metadata image.
 *
 * This class is thread-safe.
 */
public final class ClientQuotasImage {
    public final static ClientQuotasImage EMPTY = new ClientQuotasImage(Collections.emptyMap());

    private final Map<ClientQuotaEntity, ClientQuotaImage> entities;

    public ClientQuotasImage(Map<ClientQuotaEntity, ClientQuotaImage> entities) {
        this.entities = Collections.unmodifiableMap(entities);
    }

    public boolean isEmpty() {
        return entities.isEmpty();
    }

    // Visible for testing
    public Map<ClientQuotaEntity, ClientQuotaImage> entities() {
        return entities;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        for (Entry<ClientQuotaEntity, ClientQuotaImage> entry : entities.entrySet()) {
            ClientQuotaEntity entity = entry.getKey();
            ClientQuotaImage clientQuotaImage = entry.getValue();
            clientQuotaImage.write(entity, writer, options);
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
        // TODO: this is O(N). We should add indexing here to speed it up. See KAFKA-13022.
        for (Entry<ClientQuotaEntity, ClientQuotaImage> entry : entities.entrySet()) {
            ClientQuotaEntity entity = entry.getKey();
            ClientQuotaImage quotaImage = entry.getValue();
            if (matches(entity, exactMatch, typeMatch, request.strict())) {
                response.entries().add(toDescribeEntry(entity, quotaImage));
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
        if (!(o instanceof ClientQuotasImage)) return false;
        ClientQuotasImage other = (ClientQuotasImage) o;
        return entities.equals(other.entities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entities);
    }

    @Override
    public String toString() {
        return "ClientQuotasImage(entities=" + entities.entrySet().stream().
            map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", ")) +
            ")";
    }
}

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

package org.apache.kafka.controller;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.mutable.BoundedList;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.common.quota.ClientQuotaEntity.CLIENT_ID;
import static org.apache.kafka.common.quota.ClientQuotaEntity.IP;
import static org.apache.kafka.common.quota.ClientQuotaEntity.USER;
import static org.apache.kafka.controller.QuorumController.MAX_RECORDS_PER_USER_OP;


public class ClientQuotaControlManager {
    static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        ClientQuotaControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            return new ClientQuotaControlManager(logContext, snapshotRegistry);
        }
    }

    private final Logger log;

    private final SnapshotRegistry snapshotRegistry;

    final TimelineHashMap<ClientQuotaEntity, TimelineHashMap<String, Double>> clientQuotaData;

    ClientQuotaControlManager(
        LogContext logContext,
        SnapshotRegistry snapshotRegistry
    ) {
        this.log = logContext.logger(ClientQuotaControlManager.class);
        this.snapshotRegistry = snapshotRegistry;
        this.clientQuotaData = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * Determine the result of applying a batch of client quota alteration.  Note
     * that this method does not change the contents of memory.  It just generates a
     * result, that you can replay later if you wish using replay().
     *
     * @param quotaAlterations  List of client quota alterations to evaluate
     * @return                  The result.
     */
    ControllerResult<Map<ClientQuotaEntity, ApiError>> alterClientQuotas(
            Collection<ClientQuotaAlteration> quotaAlterations) {
        List<ApiMessageAndVersion> outputRecords =
                BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        Map<ClientQuotaEntity, ApiError> outputResults = new HashMap<>();

        quotaAlterations.forEach(quotaAlteration -> {
            // Note that the values in this map may be null
            Map<String, Double> alterations = new HashMap<>(quotaAlteration.ops().size());
            quotaAlteration.ops().forEach(op -> {
                if (alterations.containsKey(op.key())) {
                    outputResults.put(quotaAlteration.entity(), ApiError.fromThrowable(
                            new InvalidRequestException("Duplicate quota key " + op.key() +
                                " not updating quota for this entity " + quotaAlteration.entity())));
                } else {
                    alterations.put(op.key(), op.value());
                }
            });
            if (outputResults.containsKey(quotaAlteration.entity())) {
                outputResults.put(quotaAlteration.entity(), ApiError.fromThrowable(
                        new InvalidRequestException("Ignoring duplicate entity " + quotaAlteration.entity())));
            } else {
                alterClientQuotaEntity(quotaAlteration.entity(), alterations, outputRecords, outputResults);
            }
        });

        return ControllerResult.atomicOf(outputRecords, outputResults);
    }

    public Map<ClientQuotaEntity, Map<String, Double>> describeClientQuotas(
            long lastCommittedOffset, ClientQuotaFilter quotaFilter) {
        Map<ClientQuotaEntity, Map<String, Double>> results = new HashMap<>();
        Map<String, String> exactMatch = new HashMap<>();
        Set<String> typeMatch = new HashSet<>();

        for (ClientQuotaFilterComponent component : quotaFilter.components()) {
            validate(exactMatch, typeMatch, component.entityType(), component);

            Optional<String> match = component.match();

            if (match == null) {
                exactMatch.put(component.entityType(), null);
            } else if (component.match().isPresent()) {
                exactMatch.put(component.entityType(), component.match().get());
            } else {
                typeMatch.add(component.entityType());
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

        for (Entry<ClientQuotaEntity, TimelineHashMap<String, Double>> entry : clientQuotaData.entrySet(lastCommittedOffset)) {
            ClientQuotaEntity entity = entry.getKey();
            TimelineHashMap<String, Double> quotaImage = entry.getValue();
            if (matches(entity, exactMatch, typeMatch, quotaFilter.strict())) {
                results.put(entity, new HashMap<>(quotaImage));
            }
        }

        return results;
    }

    private static void validate(Map<String, String> exactMatch, Set<String> typeMatch, String s, ClientQuotaFilterComponent component) {
        if (s.isEmpty()) {
            throw new InvalidRequestException("Invalid empty entity type.");
        } else if (exactMatch.containsKey(s) ||
                typeMatch.contains(s)) {
            throw new InvalidRequestException("Entity type " + s +
                    " cannot appear more than once in the filter.");
        }
        if (!(s.equals(IP) || s.equals(USER) ||
                s.equals(CLIENT_ID))) {
            throw new UnsupportedVersionException("Unsupported entity type " +
                    s);
        }
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

    /**
     * Apply a quota record to the in-memory state.
     *
     * @param record    A ClientQuotaRecord instance.
     */
    public void replay(ClientQuotaRecord record) {
        Map<String, String> entityMap = new HashMap<>(2);
        record.entity().forEach(entityData -> entityMap.put(entityData.entityType(), entityData.entityName()));
        ClientQuotaEntity entity = new ClientQuotaEntity(entityMap);
        TimelineHashMap<String, Double> quotas = clientQuotaData.get(entity);
        if (quotas == null) {
            quotas = new TimelineHashMap<>(snapshotRegistry, 0);
            clientQuotaData.put(entity, quotas);
        }
        if (record.remove()) {
            quotas.remove(record.key());
            if (quotas.isEmpty()) {
                clientQuotaData.remove(entity);
            }
            log.info("Replayed ClientQuotaRecord for {} removing {}.", entity, record.key());
        } else {
            quotas.put(record.key(), record.value());
            log.info("Replayed ClientQuotaRecord for {} setting {} to {}.",
                    entity, record.key(), record.value());
        }
    }

    private void alterClientQuotaEntity(
        ClientQuotaEntity entity,
        Map<String, Double> newQuotaConfigs,
        List<ApiMessageAndVersion> outputRecords,
        Map<ClientQuotaEntity, ApiError> outputResults
    ) {
        // Check entity types and sanitize the names
        Map<String, String> validatedEntityMap = new HashMap<>(3);
        ApiError error = validateEntity(entity, validatedEntityMap);
        if (error.isFailure()) {
            outputResults.put(entity, error);
            return;
        }

        // Check the combination of entity types and get the config keys
        Map<String, ConfigDef.ConfigKey> configKeys = new HashMap<>(4);
        error = configKeysForEntityType(validatedEntityMap, configKeys);
        if (error.isFailure()) {
            outputResults.put(entity, error);
            return;
        }

        // Don't share objects between different records
        Supplier<List<EntityData>> recordEntitySupplier = () ->
                validatedEntityMap.entrySet().stream().map(mapEntry -> new EntityData()
                        .setEntityType(mapEntry.getKey())
                        .setEntityName(mapEntry.getValue()))
                        .collect(Collectors.toList());

        List<ApiMessageAndVersion> newRecords = new ArrayList<>(newQuotaConfigs.size());
        Map<String, Double> currentQuotas = clientQuotaData.containsKey(entity) ?
                clientQuotaData.get(entity) : Collections.emptyMap();
        for (Map.Entry<String, Double> entry : newQuotaConfigs.entrySet()) {
            String key = entry.getKey();
            Double newValue = entry.getValue();
            if (newValue == null) {
                if (currentQuotas.containsKey(key)) {
                    // Null value indicates removal
                    newRecords.add(new ApiMessageAndVersion(new ClientQuotaRecord()
                            .setEntity(recordEntitySupplier.get())
                            .setKey(key)
                            .setRemove(true),
                        (short) 0));
                }
            } else {
                ApiError validationError = validateQuotaKeyValue(configKeys, key, newValue);
                if (validationError.isFailure()) {
                    outputResults.put(entity, validationError);
                    return;
                } else {
                    final Double currentValue = currentQuotas.get(key);
                    if (!Objects.equals(currentValue, newValue)) {
                        // Only record the new value if it has changed
                        newRecords.add(new ApiMessageAndVersion(new ClientQuotaRecord()
                                .setEntity(recordEntitySupplier.get())
                                .setKey(key)
                                .setValue(newValue),
                            (short) 0));
                    }
                }
            }
        }

        outputRecords.addAll(newRecords);
        outputResults.put(entity, ApiError.NONE);
    }

    static ApiError configKeysForEntityType(Map<String, String> entity, Map<String, ConfigDef.ConfigKey> output) {
        // We only allow certain combinations of quota entity types. Which type is in use determines which config
        // keys are valid
        boolean hasUser = entity.containsKey(ClientQuotaEntity.USER);
        boolean hasClientId = entity.containsKey(ClientQuotaEntity.CLIENT_ID);
        boolean hasIp = entity.containsKey(ClientQuotaEntity.IP);

        final Map<String, ConfigDef.ConfigKey> configKeys;
        if (hasIp) {
            if (hasUser || hasClientId) {
                return new ApiError(Errors.INVALID_REQUEST, "Invalid quota entity combination, IP entity should" +
                    "not be combined with User or ClientId");
            } else {
                if (isValidIpEntity(entity.get(ClientQuotaEntity.IP))) {
                    configKeys = QuotaConfig.ipConfigs().configKeys();
                } else {
                    return new ApiError(Errors.INVALID_REQUEST, entity.get(ClientQuotaEntity.IP) + " is not a valid IP or resolvable host.");
                }
            }
        } else if (hasUser || hasClientId) {
            configKeys = QuotaConfig.userAndClientQuotaConfigs().configKeys();
        } else {
            return new ApiError(Errors.INVALID_REQUEST, "Invalid empty client quota entity");
        }

        output.putAll(configKeys);
        return ApiError.NONE;
    }

    static ApiError validateQuotaKeyValue(
        Map<String, ConfigDef.ConfigKey> validKeys,
        String key,
        double value
    ) {
        // Ensure we have an allowed quota key
        ConfigDef.ConfigKey configKey = validKeys.get(key);
        if (configKey == null) {
            return new ApiError(Errors.INVALID_REQUEST, "Invalid configuration key " + key);
        }
        if (value <= 0.0) {
            return new ApiError(Errors.INVALID_REQUEST, "Quota " + key + " must be greater than 0");
        }

        // Ensure the quota value is valid
        switch (configKey.type()) {
            case DOUBLE:
                return ApiError.NONE;
            case SHORT:
                if (value > Short.MAX_VALUE) {
                    return new ApiError(Errors.INVALID_REQUEST,
                        "Proposed value for " + key + " is too large for a SHORT.");
                }
                return getErrorForIntegralQuotaValue(value, key);
            case INT:
                if (value > Integer.MAX_VALUE) {
                    return new ApiError(Errors.INVALID_REQUEST,
                        "Proposed value for " + key + " is too large for an INT.");
                }
                return getErrorForIntegralQuotaValue(value, key);
            case LONG: {
                if (value > Long.MAX_VALUE) {
                    return new ApiError(Errors.INVALID_REQUEST,
                        "Proposed value for " + key + " is too large for a LONG.");
                }
                return getErrorForIntegralQuotaValue(value, key);
            }
            default:
                return new ApiError(Errors.UNKNOWN_SERVER_ERROR,
                        "Unexpected config type " + configKey.type() + " should be Long or Double");
        }
    }

    static ApiError getErrorForIntegralQuotaValue(double value, String key) {
        double remainder = Math.abs(value % 1.0);
        if (remainder > 1e-6) {
            return new ApiError(Errors.INVALID_REQUEST, key + " cannot be a fractional value.");
        }
        return ApiError.NONE;
    }

    static boolean isValidIpEntity(String ip) {
        if (ip == null) return true;
        try {
            InetAddress.getByName(ip);
            return true;
        } catch (UnknownHostException e) {
            return false;
        }
    }

    private ApiError validateEntity(ClientQuotaEntity entity, Map<String, String> validatedEntityMap) {
        // Given a quota entity (which is a mapping of entity type to entity name), validate its types
        if (entity.entries().isEmpty()) {
            return new ApiError(Errors.INVALID_REQUEST, "Invalid empty client quota entity");
        }

        for (Entry<String, String> entityEntry : entity.entries().entrySet()) {
            String entityType = entityEntry.getKey();
            String entityName = entityEntry.getValue();
            if (validatedEntityMap.containsKey(entityType)) {
                return new ApiError(Errors.INVALID_REQUEST, "Invalid client quota entity, duplicate entity entry " + entityType);
            }
            if (Objects.equals(entityType, ClientQuotaEntity.USER)) {
                validatedEntityMap.put(ClientQuotaEntity.USER, entityName);
            } else if (Objects.equals(entityType, ClientQuotaEntity.CLIENT_ID)) {
                validatedEntityMap.put(ClientQuotaEntity.CLIENT_ID, entityName);
            } else if (Objects.equals(entityType, ClientQuotaEntity.IP)) {
                validatedEntityMap.put(ClientQuotaEntity.IP, entityName);
            } else {
                return new ApiError(Errors.INVALID_REQUEST, "Unhandled client quota entity type: " + entityType);
            }

            if (entityName != null && entityName.isEmpty()) {
                return new ApiError(Errors.INVALID_REQUEST, "Empty " + entityType + " not supported");
            }
        }

        return ApiError.NONE;
    }
}

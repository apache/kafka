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
import org.apache.kafka.common.config.internals.QuotaConfigs;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.common.metadata.MetadataRecordType.CLIENT_QUOTA_RECORD;


public class ClientQuotaControlManager {
    private final SnapshotRegistry snapshotRegistry;

    final TimelineHashMap<ClientQuotaEntity, TimelineHashMap<String, Double>> clientQuotaData;

    ClientQuotaControlManager(SnapshotRegistry snapshotRegistry) {
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
        List<ApiMessageAndVersion> outputRecords = new ArrayList<>();
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
            if (quotas.size() == 0) {
                clientQuotaData.remove(entity);
            }
        } else {
            quotas.put(record.key(), record.value());
        }
    }

    private void alterClientQuotaEntity(
            ClientQuotaEntity entity,
            Map<String, Double> newQuotaConfigs,
            List<ApiMessageAndVersion> outputRecords,
            Map<ClientQuotaEntity, ApiError> outputResults) {

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
                        CLIENT_QUOTA_RECORD.highestSupportedVersion()));
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
                            CLIENT_QUOTA_RECORD.highestSupportedVersion()));
                    }
                }
            }
        }

        outputRecords.addAll(newRecords);
        outputResults.put(entity, ApiError.NONE);
    }

    private ApiError configKeysForEntityType(Map<String, String> entity, Map<String, ConfigDef.ConfigKey> output) {
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
                    configKeys = QuotaConfigs.ipConfigs().configKeys();
                } else {
                    return new ApiError(Errors.INVALID_REQUEST, entity.get(ClientQuotaEntity.IP) + " is not a valid IP or resolvable host.");
                }
            }
        } else if (hasUser && hasClientId) {
            configKeys = QuotaConfigs.userConfigs().configKeys();
        } else if (hasUser) {
            configKeys = QuotaConfigs.userConfigs().configKeys();
        } else if (hasClientId) {
            configKeys = QuotaConfigs.clientConfigs().configKeys();
        } else {
            return new ApiError(Errors.INVALID_REQUEST, "Invalid empty client quota entity");
        }

        output.putAll(configKeys);
        return ApiError.NONE;
    }

    private ApiError validateQuotaKeyValue(Map<String, ConfigDef.ConfigKey> validKeys, String key, Double value) {
        // TODO can this validation be shared with alter configs?
        // Ensure we have an allowed quota key
        ConfigDef.ConfigKey configKey = validKeys.get(key);
        if (configKey == null) {
            return new ApiError(Errors.INVALID_REQUEST, "Invalid configuration key " + key);
        }

        // Ensure the quota value is valid
        switch (configKey.type()) {
            case DOUBLE:
                break;
            case SHORT:
            case INT:
            case LONG:
                Double epsilon = 1e-6;
                Long longValue = Double.valueOf(value + epsilon).longValue();
                if (Math.abs(longValue.doubleValue() - value) > epsilon) {
                    return new ApiError(Errors.INVALID_REQUEST,
                            "Configuration " + key + " must be a Long value");
                }
                break;
            default:
                return new ApiError(Errors.UNKNOWN_SERVER_ERROR,
                        "Unexpected config type " + configKey.type() + " should be Long or Double");
        }
        return ApiError.NONE;
    }

    // TODO move this somewhere common?
    private boolean isValidIpEntity(String ip) {
        if (Objects.nonNull(ip)) {
            try {
                InetAddress.getByName(ip);
                return true;
            } catch (UnknownHostException e) {
                return false;
            }
        } else {
            return true;
        }
    }

    private ApiError validateEntity(ClientQuotaEntity entity, Map<String, String> validatedEntityMap) {
        // Given a quota entity (which is a mapping of entity type to entity name), validate it's types
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

    class ClientQuotaControlIterator implements Iterator<List<ApiMessageAndVersion>> {
        private final long epoch;
        private final Iterator<Entry<ClientQuotaEntity, TimelineHashMap<String, Double>>> iterator;

        ClientQuotaControlIterator(long epoch) {
            this.epoch = epoch;
            this.iterator = clientQuotaData.entrySet(epoch).iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public List<ApiMessageAndVersion> next() {
            if (!hasNext()) throw new NoSuchElementException();
            Entry<ClientQuotaEntity, TimelineHashMap<String, Double>> entry = iterator.next();
            ClientQuotaEntity entity = entry.getKey();
            List<ApiMessageAndVersion> records = new ArrayList<>();
            for (Entry<String, Double> quotaEntry : entry.getValue().entrySet(epoch)) {
                ClientQuotaRecord record = new ClientQuotaRecord();
                for (Entry<String, String> entityEntry : entity.entries().entrySet()) {
                    record.entity().add(new EntityData().
                        setEntityType(entityEntry.getKey()).
                        setEntityName(entityEntry.getValue()));
                }
                record.setKey(quotaEntry.getKey());
                record.setValue(quotaEntry.getValue());
                record.setRemove(false);
                records.add(new ApiMessageAndVersion(record,
                    CLIENT_QUOTA_RECORD.highestSupportedVersion()));
            }
            return records;
        }
    }

    ClientQuotaControlIterator iterator(long epoch) {
        return new ClientQuotaControlIterator(epoch);
    }
}

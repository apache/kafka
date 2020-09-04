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
package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.DescribeClientConfigsResponseData;
import org.apache.kafka.common.message.DescribeClientConfigsResponseData.EntityData;
import org.apache.kafka.common.message.DescribeClientConfigsResponseData.EntryData;
import org.apache.kafka.common.message.DescribeClientConfigsResponseData.ValueData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.slf4j.Logger;

public class DescribeClientConfigsResponse extends AbstractResponse {

    private final DescribeClientConfigsResponseData data;

    public DescribeClientConfigsResponse(Map<ClientQuotaEntity, Map<String, String>> entities, int throttleTimeMs) {
        List<EntryData> entries = new ArrayList<>(entities.size());
        for (Map.Entry<ClientQuotaEntity, Map<String, String>> entry : entities.entrySet()) {
            ClientQuotaEntity quotaEntity = entry.getKey();
            List<EntityData> entityData = new ArrayList<>(quotaEntity.entries().size());
            for (Map.Entry<String, String> entityEntry : quotaEntity.entries().entrySet()) {
                entityData.add(new EntityData()
                        .setEntityType(entityEntry.getKey())
                        .setEntityName(entityEntry.getValue()));
            }

            Map<String, String> configValues = entry.getValue();
            List<ValueData> valueData = new ArrayList<>(configValues.size());
            for (Map.Entry<String, String> valuesEntry : entry.getValue().entrySet()) {
                valueData.add(new ValueData()
                        .setKey(valuesEntry.getKey())
                        .setValue(valuesEntry.getValue()));
            }

            entries.add(new EntryData()
                    .setEntity(entityData)
                    .setValues(valueData));
        }

        this.data = new DescribeClientConfigsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode((short) 0)
                .setErrorMessage(null)
                .setEntries(entries);
    }

    public DescribeClientConfigsResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        this.data = new DescribeClientConfigsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message())
                .setEntries(null);
    }

    public DescribeClientConfigsResponse(Struct struct, short version) {
        this.data = new DescribeClientConfigsResponseData(struct, version);
    }

    public void complete(KafkaFutureImpl<Map<ClientQuotaEntity, Map<String, String>>> future) {
        Errors error = Errors.forCode(data.errorCode());
        if (error != Errors.NONE) {
            future.completeExceptionally(error.exception(data.errorMessage()));
            return;
        }

        Map<ClientQuotaEntity, Map<String, String>> result = new HashMap<>(data.entries().size());
        for (EntryData entries : data.entries()) {
            Map<String, String> entity = new HashMap<>(entries.entity().size());
            for (EntityData entityData : entries.entity()) {
                entity.put(entityData.entityType(), entityData.entityName());
            }

            Map<String, String> values = new HashMap<>(entries.values().size());
            for (ValueData valueData : entries.values()) {
                values.put(valueData.key(), valueData.value());
            }

            result.put(new ClientQuotaEntity(entity), values);
        }
        future.complete(result);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public short errorCode() {
        return data.errorCode();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(data.errorCode()));
    }

    public Map<String, String> resultMap(Logger log) {
        Map<String, String> result = new HashMap<>();
        data.entries().stream().forEach(entityEntry -> {
            log.info("Entity: {}", entityEntry.entity());
            log.info("Values: {}", entityEntry.values());
            Map<String, String> entryConfigs = entityEntry.values().stream()
                .collect(Collectors.toMap(ValueData::key, ValueData::value));
            result.putAll(entryConfigs);
        });
        return result;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public static DescribeClientConfigsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeClientConfigsResponse(ApiKeys.DESCRIBE_CLIENT_CONFIGS.parseResponse(version, buffer), version);
    }
}

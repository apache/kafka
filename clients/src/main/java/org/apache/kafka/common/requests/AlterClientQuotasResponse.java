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

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.AlterClientQuotasResponseData;
import org.apache.kafka.common.message.AlterClientQuotasResponseData.EntityData;
import org.apache.kafka.common.message.AlterClientQuotasResponseData.EntryData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaEntity;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterClientQuotasResponse extends AbstractResponse {

    private final AlterClientQuotasResponseData data;

    public AlterClientQuotasResponse(AlterClientQuotasResponseData data) {
        super(ApiKeys.ALTER_CLIENT_QUOTAS);
        this.data = data;
    }

    public void complete(Map<ClientQuotaEntity, KafkaFutureImpl<Void>> futures) {
        for (EntryData entryData : data.entries()) {
            Map<String, String> entityEntries = new HashMap<>(entryData.entity().size());
            for (EntityData entityData : entryData.entity()) {
                entityEntries.put(entityData.entityType(), entityData.entityName());
            }
            ClientQuotaEntity entity = new ClientQuotaEntity(entityEntries);

            KafkaFutureImpl<Void> future = futures.get(entity);
            if (future == null) {
                throw new IllegalArgumentException("Future map must contain entity " + entity);
            }

            Errors error = Errors.forCode(entryData.errorCode());
            if (error == Errors.NONE) {
                future.complete(null);
            } else {
                future.completeExceptionally(error.exception(entryData.errorMessage()));
            }
        }
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        data.entries().forEach(entry ->
            updateErrorCounts(counts, Errors.forCode(entry.errorCode()))
        );
        return counts;
    }

    @Override
    public AlterClientQuotasResponseData data() {
        return data;
    }

    private static List<EntityData> toEntityData(ClientQuotaEntity entity) {
        List<AlterClientQuotasResponseData.EntityData> entityData = new ArrayList<>(entity.entries().size());
        for (Map.Entry<String, String> entry : entity.entries().entrySet()) {
            entityData.add(new AlterClientQuotasResponseData.EntityData()
                    .setEntityType(entry.getKey())
                    .setEntityName(entry.getValue()));
        }
        return entityData;
    }

    public static AlterClientQuotasResponse parse(ByteBuffer buffer, short version) {
        return new AlterClientQuotasResponse(new AlterClientQuotasResponseData(new ByteBufferAccessor(buffer), version));
    }

    public static AlterClientQuotasResponse fromQuotaEntities(Map<ClientQuotaEntity, ApiError> result, int throttleTimeMs) {
        List<EntryData> entries = new ArrayList<>(result.size());
        for (Map.Entry<ClientQuotaEntity, ApiError> entry : result.entrySet()) {
            ApiError e = entry.getValue();
            entries.add(new EntryData()
                    .setErrorCode(e.error().code())
                    .setErrorMessage(e.message())
                    .setEntity(toEntityData(entry.getKey())));
        }

        return new AlterClientQuotasResponse(new AlterClientQuotasResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setEntries(entries));
    }

}

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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.quota.ClientQuotaEntity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterClientQuotasResponse extends AbstractResponse {

    private final AlterClientQuotasResponseData data;

    public AlterClientQuotasResponse(Map<ClientQuotaEntity, ApiError> result, int throttleTimeMs) {
        List<EntryData> entries = new ArrayList<>(result.size());
        for (Map.Entry<ClientQuotaEntity, ApiError> entry : result.entrySet()) {
            ApiError e = entry.getValue();
            entries.add(new EntryData()
                    .setErrorCode(e.error().code())
                    .setErrorMessage(e.message())
                    .setEntity(toEntityData(entry.getKey())));
        }

        this.data = new AlterClientQuotasResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setEntries(entries);
    }

    public AlterClientQuotasResponse(Collection<ClientQuotaEntity> entities, int throttleTimeMs, Throwable e) {
        short errorCode = Errors.forException(e).code();
        String errorMessage = e.getMessage();

        List<EntryData> entries = new ArrayList<>(entities.size());
        for (ClientQuotaEntity entity : entities) {
            entries.add(new EntryData()
                    .setErrorCode(errorCode)
                    .setErrorMessage(errorMessage)
                    .setEntity(toEntityData(entity)));
        }

        this.data = new AlterClientQuotasResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setEntries(entries);
    }

    public AlterClientQuotasResponse(Struct struct, short version) {
        this.data = new AlterClientQuotasResponseData(struct, version);
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
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        for (EntryData entry : data.entries()) {
            Errors error = Errors.forCode(entry.errorCode());
            counts.put(error, counts.getOrDefault(error, 0) + 1);
        }
        return counts;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
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
}

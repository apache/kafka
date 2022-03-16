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

import org.apache.kafka.common.message.AlterClientQuotasRequestData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData.EntityData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData.EntryData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData.OpData;
import org.apache.kafka.common.message.AlterClientQuotasResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterClientQuotasRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<AlterClientQuotasRequest> {

        private final AlterClientQuotasRequestData data;

        public Builder(Collection<ClientQuotaAlteration> entries, boolean validateOnly) {
            super(ApiKeys.ALTER_CLIENT_QUOTAS);

            List<EntryData> entryData = new ArrayList<>(entries.size());
            for (ClientQuotaAlteration entry : entries) {
                List<EntityData> entityData = new ArrayList<>(entry.entity().entries().size());
                for (Map.Entry<String, String> entityEntries : entry.entity().entries().entrySet()) {
                    entityData.add(new EntityData()
                            .setEntityType(entityEntries.getKey())
                            .setEntityName(entityEntries.getValue()));
                }

                List<OpData> opData = new ArrayList<>(entry.ops().size());
                for (ClientQuotaAlteration.Op op : entry.ops()) {
                    opData.add(new OpData()
                            .setKey(op.key())
                            .setValue(op.value() == null ? 0.0 : op.value())
                            .setRemove(op.value() == null));
                }

                entryData.add(new EntryData()
                        .setEntity(entityData)
                        .setOps(opData));
            }

            this.data = new AlterClientQuotasRequestData()
                    .setEntries(entryData)
                    .setValidateOnly(validateOnly);
        }

        @Override
        public AlterClientQuotasRequest build(short version) {
            return new AlterClientQuotasRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final AlterClientQuotasRequestData data;

    public AlterClientQuotasRequest(AlterClientQuotasRequestData data, short version) {
        super(ApiKeys.ALTER_CLIENT_QUOTAS, version);
        this.data = data;
    }

    public List<ClientQuotaAlteration> entries() {
        List<ClientQuotaAlteration> entries = new ArrayList<>(data.entries().size());
        for (EntryData entryData : data.entries()) {
            Map<String, String> entity = new HashMap<>(entryData.entity().size());
            for (EntityData entityData : entryData.entity()) {
                entity.put(entityData.entityType(), entityData.entityName());
            }

            List<ClientQuotaAlteration.Op> ops = new ArrayList<>(entryData.ops().size());
            for (OpData opData : entryData.ops()) {
                Double value = opData.remove() ? null : opData.value();
                ops.add(new ClientQuotaAlteration.Op(opData.key(), value));
            }

            entries.add(new ClientQuotaAlteration(new ClientQuotaEntity(entity), ops));
        }
        return entries;
    }

    public boolean validateOnly() {
        return data.validateOnly();
    }

    @Override
    public AlterClientQuotasRequestData data() {
        return data;
    }

    @Override
    public AlterClientQuotasResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        List<AlterClientQuotasResponseData.EntryData> responseEntries = new ArrayList<>();
        for (EntryData entryData : data.entries()) {
            List<AlterClientQuotasResponseData.EntityData> responseEntities = new ArrayList<>();
            for (EntityData entityData : entryData.entity()) {
                responseEntities.add(new AlterClientQuotasResponseData.EntityData()
                    .setEntityType(entityData.entityType())
                    .setEntityName(entityData.entityName()));
            }
            responseEntries.add(new AlterClientQuotasResponseData.EntryData()
                .setEntity(responseEntities)
                .setErrorCode(error.code())
                .setErrorMessage(error.message()));
        }
        AlterClientQuotasResponseData responseData = new AlterClientQuotasResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setEntries(responseEntries);
        return new AlterClientQuotasResponse(responseData);
    }

    public static AlterClientQuotasRequest parse(ByteBuffer buffer, short version) {
        return new AlterClientQuotasRequest(new AlterClientQuotasRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}

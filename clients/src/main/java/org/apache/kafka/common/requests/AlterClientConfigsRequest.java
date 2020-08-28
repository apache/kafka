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

import org.apache.kafka.common.message.AlterClientConfigsRequestData;
import org.apache.kafka.common.message.AlterClientConfigsRequestData.EntityData;
import org.apache.kafka.common.message.AlterClientConfigsRequestData.EntryData;
import org.apache.kafka.common.message.AlterClientConfigsRequestData.OpData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.config.ClientConfigAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterClientConfigsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<AlterClientConfigsRequest> {

        private final AlterClientConfigsRequestData data;

        public Builder(Collection<ClientConfigAlteration> entries, boolean validateOnly) {
            super(ApiKeys.ALTER_CLIENT_CONFIGS);

            List<EntryData> entryData = new ArrayList<>(entries.size());
            for (ClientConfigAlteration entry : entries) {
                List<EntityData> entityData = new ArrayList<>(entry.entity().entries().size());
                for (Map.Entry<String, String> entityEntries : entry.entity().entries().entrySet()) {
                    entityData.add(new EntityData()
                            .setEntityType(entityEntries.getKey())
                            .setEntityName(entityEntries.getValue()));
                }

                List<OpData> opData = new ArrayList<>(entry.ops().size());
                for (ClientConfigAlteration.Op op : entry.ops()) {
                    opData.add(new OpData()
                            .setKey(op.key())
                            .setValue(op.value() == null ? "" : op.value())
                            .setRemove(op.value() == null));
                }

                entryData.add(new EntryData()
                        .setEntity(entityData)
                        .setOps(opData));
            }

            this.data = new AlterClientConfigsRequestData()
                    .setEntries(entryData)
                    .setValidateOnly(validateOnly);
        }

        @Override
        public AlterClientConfigsRequest build(short version) {
            return new AlterClientConfigsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final AlterClientConfigsRequestData data;

    public AlterClientConfigsRequest(AlterClientConfigsRequestData data, short version) {
        super(ApiKeys.ALTER_CLIENT_CONFIGS, version);
        this.data = data;
    }

    public AlterClientConfigsRequest(Struct struct, short version) {
        super(ApiKeys.ALTER_CLIENT_CONFIGS, version);
        this.data = new AlterClientConfigsRequestData(struct, version);
    }

    public Collection<ClientConfigAlteration> entries() {
        List<ClientConfigAlteration> entries = new ArrayList<>(data.entries().size());
        for (EntryData entryData : data.entries()) {
            Map<String, String> entity = new HashMap<>(entryData.entity().size());
            for (EntityData entityData : entryData.entity()) {
                entity.put(entityData.entityType(), entityData.entityName());
            }

            List<ClientConfigAlteration.Op> ops = new ArrayList<>(entryData.ops().size());
            for (OpData opData : entryData.ops()) {
                String value = opData.remove() ? null : opData.value();
                ops.add(new ClientConfigAlteration.Op(opData.key(), value));
            }

            entries.add(new ClientConfigAlteration(new ClientQuotaEntity(entity), ops));
        }
        return entries;
    }

    public boolean validateOnly() {
        return data.validateOnly();
    }

    @Override
    public AlterClientConfigsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ArrayList<ClientQuotaEntity> entities = new ArrayList<>(data.entries().size());
        for (EntryData entryData : data.entries()) {
            Map<String, String> entity = new HashMap<>(entryData.entity().size());
            for (EntityData entityData : entryData.entity()) {
                entity.put(entityData.entityType(), entityData.entityName());
            }
            entities.add(new ClientQuotaEntity(entity));
        }
        return new AlterClientConfigsResponse(entities, throttleTimeMs, e);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }
}

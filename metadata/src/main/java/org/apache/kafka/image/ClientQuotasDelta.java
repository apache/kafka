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

import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


public final class ClientQuotasDelta {
    private final ClientQuotasImage image;
    private final Map<ClientQuotaEntity, ClientQuotaDelta> changes = new HashMap<>();

    public ClientQuotasDelta(ClientQuotasImage image) {
        this.image = image;
    }

    public Map<ClientQuotaEntity, ClientQuotaDelta> changes() {
        return changes;
    }

    public void finishSnapshot() {
        for (Entry<ClientQuotaEntity, ClientQuotaImage> entry : image.entities().entrySet()) {
            ClientQuotaEntity entity = entry.getKey();
            ClientQuotaImage quotaImage = entry.getValue();
            ClientQuotaDelta quotaDelta = changes.computeIfAbsent(entity,
                __ -> new ClientQuotaDelta(quotaImage));
            quotaDelta.finishSnapshot();
        }
    }

    public void handleMetadataVersionChange(MetadataVersion newVersion) {
        // no-op
    }

    public void replay(ClientQuotaRecord record) {
        ClientQuotaEntity entity = ClientQuotaImage.dataToEntity(record.entity());
        ClientQuotaDelta change = changes.computeIfAbsent(entity, __ ->
            new ClientQuotaDelta(image.entities().
                getOrDefault(entity, ClientQuotaImage.EMPTY)));
        change.replay(record);
    }

    public ClientQuotasImage apply() {
        Map<ClientQuotaEntity, ClientQuotaImage> newEntities =
            new HashMap<>(image.entities().size());
        for (Entry<ClientQuotaEntity, ClientQuotaImage> entry : image.entities().entrySet()) {
            ClientQuotaEntity entity = entry.getKey();
            ClientQuotaDelta change = changes.get(entity);
            if (change == null) {
                newEntities.put(entity, entry.getValue());
            } else {
                ClientQuotaImage quotaImage = change.apply();
                if (!quotaImage.isEmpty()) {
                    newEntities.put(entity, quotaImage);
                }
            }
        }
        for (Entry<ClientQuotaEntity, ClientQuotaDelta> entry : changes.entrySet()) {
            ClientQuotaEntity entity = entry.getKey();
            if (!newEntities.containsKey(entity)) {
                ClientQuotaImage quotaImage = entry.getValue().apply();
                if (!quotaImage.isEmpty()) {
                    newEntities.put(entity, quotaImage);
                }
            }
        }
        return new ClientQuotasImage(newEntities);
    }

    @Override
    public String toString() {
        return "ClientQuotasDelta(" +
            "changes=" + changes +
            ')';
    }
}

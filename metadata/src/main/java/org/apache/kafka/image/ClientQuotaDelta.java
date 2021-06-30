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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Map.Entry;


public final class ClientQuotaDelta {
    private final ClientQuotaImage image;
    private final Map<String, Optional<Double>> changes = new HashMap<>();

    public ClientQuotaDelta(ClientQuotaImage image) {
        this.image = image;
    }

    public Map<String, Optional<Double>> changes() {
        return changes;
    }

    public void finishSnapshot() {
        for (String key : image.quotas().keySet()) {
            if (!changes.containsKey(key)) {
                changes.put(key, Optional.empty());
            }
        }
    }

    public void replay(ClientQuotaRecord record) {
        if (record.remove()) {
            changes.put(record.key(), Optional.empty());
        } else {
            changes.put(record.key(), Optional.of(record.value()));
        }
    }

    public ClientQuotaImage apply() {
        Map<String, Double> newQuotas = new HashMap<>(image.quotas().size());
        for (Entry<String, Double> entry : image.quotas().entrySet()) {
            Optional<Double> change = changes.get(entry.getKey());
            if (change == null) {
                newQuotas.put(entry.getKey(), entry.getValue());
            } else if (change.isPresent()) {
                newQuotas.put(entry.getKey(), change.get());
            }
        }
        for (Entry<String, Optional<Double>> entry : changes.entrySet()) {
            if (!newQuotas.containsKey(entry.getKey())) {
                if (entry.getValue().isPresent()) {
                    newQuotas.put(entry.getKey(), entry.getValue().get());
                }
            }
        }
        return new ClientQuotaImage(newQuotas);
    }
}

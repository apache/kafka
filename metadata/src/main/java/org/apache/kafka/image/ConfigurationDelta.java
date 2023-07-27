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

import org.apache.kafka.common.metadata.ConfigRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;


/**
 * Represents changes to the configurations in the metadata image.
 */
public final class ConfigurationDelta {
    private final ConfigurationImage image;
    private final Map<String, Optional<String>> changes = new HashMap<>();

    public ConfigurationDelta(ConfigurationImage image) {
        this.image = image;
    }

    public void finishSnapshot() {
        for (String key : image.data().keySet()) {
            if (!changes.containsKey(key)) {
                changes.put(key, Optional.empty());
            }
        }
    }

    public void replay(ConfigRecord record) {
        changes.put(record.name(), Optional.ofNullable(record.value()));
    }

    public void deleteAll() {
        changes.clear();
        for (String key : image.data().keySet()) {
            changes.put(key, Optional.empty());
        }
    }

    public ConfigurationImage apply() {
        Map<String, String> newData = new HashMap<>(image.data().size());
        for (Entry<String, String> entry : image.data().entrySet()) {
            Optional<String> change = changes.get(entry.getKey());
            if (change == null) {
                newData.put(entry.getKey(), entry.getValue());
            } else if (change.isPresent()) {
                newData.put(entry.getKey(), change.get());
            }
        }
        for (Entry<String, Optional<String>> entry : changes.entrySet()) {
            if (!newData.containsKey(entry.getKey())) {
                if (entry.getValue().isPresent()) {
                    newData.put(entry.getKey(), entry.getValue().get());
                }
            }
        }
        return new ConfigurationImage(image.resource(), newData);
    }

    @Override
    public String toString() {
        // Values are intentionally left out of this so that sensitive configs
        // do not end up in logging by mistake.
        return "ConfigurationDelta(" +
            "changedKeys=" + changes.keySet() +
            ')';
    }
}

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

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.metadata.RemoveUserScramCredentialRecord;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.metadata.ScramCredentialData;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Optional;


/**
 * Represents changes to a topic in the metadata image.
 */
public final class ScramDelta {
    private final ScramImage image;

    private final Map<ScramMechanism, Map<String, Optional<ScramCredentialData>>> changes = new HashMap<>();

    public ScramDelta(ScramImage image) {
        this.image = image;
    }

    public void finishSnapshot() {
        for (Entry<ScramMechanism, Map<String, ScramCredentialData>> mechanismEntry : image.mechanisms().entrySet()) {
            Map<String, Optional<ScramCredentialData>> userNameMap =
                changes.computeIfAbsent(mechanismEntry.getKey(), __ -> new HashMap<>());
            for (String userName : mechanismEntry.getValue().keySet()) {
                if (!userNameMap.containsKey(userName)) {
                    userNameMap.put(userName, Optional.empty());
                }
            }
        }
    }

    public ScramImage image() {
        return image;
    }

    public Map<ScramMechanism, Map<String, Optional<ScramCredentialData>>> changes() {
        return changes;
    }

    public void replay(UserScramCredentialRecord record) {
        ScramMechanism mechanism = ScramMechanism.fromType(record.mechanism());
        Map<String, Optional<ScramCredentialData>> userChanges =
                changes.computeIfAbsent(mechanism, __ -> new HashMap<>());
        userChanges.put(record.name(), Optional.of(ScramCredentialData.fromRecord(record)));
    }

    public void replay(RemoveUserScramCredentialRecord record) {
        ScramMechanism mechanism = ScramMechanism.fromType(record.mechanism());
        Map<String, Optional<ScramCredentialData>> userChanges =
                changes.computeIfAbsent(mechanism, __ -> new HashMap<>());
        userChanges.put(record.name(), Optional.empty());
    }

    public void handleMetadataVersionChange(MetadataVersion changedMetadataVersion) {
        // nothing to do
    }

    public ScramImage apply() {
        Map<ScramMechanism, Map<String, ScramCredentialData>> newMechanisms = new HashMap<>();
        for (Entry<ScramMechanism, Map<String, ScramCredentialData>> mechanismEntry : image.mechanisms().entrySet()) {
            newMechanisms.put(mechanismEntry.getKey(), new HashMap<>(mechanismEntry.getValue()));
        }
        for (Entry<ScramMechanism, Map<String, Optional<ScramCredentialData>>> mechanismChangeEntry : changes.entrySet()) {
            Map<String, ScramCredentialData> userMap =
                    newMechanisms.computeIfAbsent(mechanismChangeEntry.getKey(), __ -> new HashMap<>());
            for (Entry<String, Optional<ScramCredentialData>> userNameEntry : mechanismChangeEntry.getValue().entrySet()) {
                if (userNameEntry.getValue().isPresent()) {
                    userMap.put(userNameEntry.getKey(), userNameEntry.getValue().get());
                } else {
                    userMap.remove(userNameEntry.getKey());
                    if (userMap.isEmpty()) {
                        newMechanisms.remove(mechanismChangeEntry.getKey());
                    }
                }
            }
        }
        return new ScramImage(newMechanisms);
    }

    @Override
    public String toString() {
        return "ScramDelta(" +
            "changes=" + changes +
            ')';
    }
}

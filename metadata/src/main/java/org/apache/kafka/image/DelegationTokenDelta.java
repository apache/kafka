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

import org.apache.kafka.common.metadata.DelegationTokenRecord;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.metadata.DelegationTokenData;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Map;
// import java.util.Optional;


/**
 * Represents changes to delegation tokens in the metadata image.
 */
public final class DelegationTokenDelta {
    private final DelegationTokenImage image;

    private final Map<String, DelegationTokenData> changes = new HashMap<>();

    public DelegationTokenDelta(DelegationTokenImage image) {
        this.image = image;
    }

    public void finishSnapshot() {
        // Not sure there is anything to do here
    }

    public DelegationTokenImage image() {
        return image;
    }

    //XXX Map uuid to Data?
    public Map<String, DelegationTokenData> changes() {
        return changes;
    }

    public void replay(DelegationTokenRecord record) {
        changes.put(record.tokenId(), DelegationTokenData.fromRecord(record));
    }

    public void handleMetadataVersionChange(MetadataVersion changedMetadataVersion) {
        // nothing to do
    }

    public DelegationTokenImage apply() {
        Map<String, DelegationTokenData> newTokens = new HashMap<>();
        for (Entry<String, DelegationTokenData> tokenChange : changes.entrySet()) {
            newTokens.put(tokenChange.getKey(), tokenChange.getValue());
            /*
            if (userNameEntry.getValue().isPresent()) {
                userMap.put(userNameEntry.getKey(), userNameEntry.getValue().get());
            } else {
                userMap.remove(userNameEntry.getKey());
                if (userMap.isEmpty()) {
                    newMechanisms.remove(mechanismChangeEntry.getKey());
                }
            }
            */
        }
        return new DelegationTokenImage(newTokens);
    }

    @Override
    public String toString() {
        return "DelegationTokenDelta(" +
            "changes=" + changes +
            ')';
    }
}

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

import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;


/**
 * Represents changes to the cluster in the metadata image.
 */
public final class ClusterDelta {
    private final ClusterImage image;
    private final HashMap<Integer, Optional<BrokerRegistration>> changedBrokers = new HashMap<>();

    public ClusterDelta(ClusterImage image) {
        this.image = image;
    }

    public HashMap<Integer, Optional<BrokerRegistration>> changedBrokers() {
        return changedBrokers;
    }

    public BrokerRegistration broker(int nodeId) {
        Optional<BrokerRegistration> result = changedBrokers.get(nodeId);
        if (result != null) {
            return result.orElse(null);
        }
        return image.broker(nodeId);
    }

    public void finishSnapshot() {
        for (Integer brokerId : image.brokers().keySet()) {
            if (!changedBrokers.containsKey(brokerId)) {
                changedBrokers.put(brokerId, Optional.empty());
            }
        }
    }

    public void handleMetadataVersionChange(MetadataVersion newVersion) {
        // no-op
    }

    public void replay(RegisterBrokerRecord record) {
        BrokerRegistration broker = BrokerRegistration.fromRecord(record);
        changedBrokers.put(broker.id(), Optional.of(broker));
    }

    public void replay(UnregisterBrokerRecord record) {
        changedBrokers.put(record.brokerId(), Optional.empty());
    }

    private BrokerRegistration getBrokerOrThrow(int brokerId, long epoch, String action) {
        BrokerRegistration broker = broker(brokerId);
        if (broker == null) {
            throw new IllegalStateException("Tried to " + action + " broker " + brokerId +
                ", but that broker was not registered.");
        }
        if (broker.epoch() != epoch) {
            throw new IllegalStateException("Tried to " + action + " broker " + brokerId +
                ", but the given epoch, " + epoch + ", did not match the current broker " +
                "epoch, " + broker.epoch());
        }
        return broker;
    }

    public void replay(FenceBrokerRecord record) {
        BrokerRegistration curRegistration = getBrokerOrThrow(record.id(), record.epoch(), "fence");
        changedBrokers.put(record.id(), Optional.of(curRegistration.cloneWith(
            BrokerRegistrationFencingChange.FENCE.asBoolean(),
            Optional.empty()
        )));
    }

    public void replay(UnfenceBrokerRecord record) {
        BrokerRegistration curRegistration = getBrokerOrThrow(record.id(), record.epoch(), "unfence");
        changedBrokers.put(record.id(), Optional.of(curRegistration.cloneWith(
            BrokerRegistrationFencingChange.UNFENCE.asBoolean(),
            Optional.empty()
        )));
    }

    public void replay(BrokerRegistrationChangeRecord record) {
        BrokerRegistration curRegistration =
            getBrokerOrThrow(record.brokerId(), record.brokerEpoch(), "change");
        BrokerRegistrationFencingChange fencingChange =
            BrokerRegistrationFencingChange.fromValue(record.fenced()).orElseThrow(
                () -> new IllegalStateException(String.format("Unable to replay %s: unknown " +
                    "value for fenced field: %d", record, record.fenced())));
        BrokerRegistrationInControlledShutdownChange inControlledShutdownChange =
            BrokerRegistrationInControlledShutdownChange.fromValue(record.inControlledShutdown()).orElseThrow(
                () -> new IllegalStateException(String.format("Unable to replay %s: unknown " +
                    "value for inControlledShutdown field: %d", record, record.inControlledShutdown())));
        BrokerRegistration nextRegistration = curRegistration.cloneWith(
            fencingChange.asBoolean(),
            inControlledShutdownChange.asBoolean()
        );
        if (!curRegistration.equals(nextRegistration)) {
            changedBrokers.put(record.brokerId(), Optional.of(nextRegistration));
        }
    }

    public ClusterImage apply() {
        Map<Integer, BrokerRegistration> newBrokers = new HashMap<>(image.brokers().size());
        for (Entry<Integer, BrokerRegistration> entry : image.brokers().entrySet()) {
            int nodeId = entry.getKey();
            Optional<BrokerRegistration> change = changedBrokers.get(nodeId);
            if (change == null) {
                newBrokers.put(nodeId, entry.getValue());
            } else if (change.isPresent()) {
                newBrokers.put(nodeId, change.get());
            }
        }
        for (Entry<Integer, Optional<BrokerRegistration>> entry : changedBrokers.entrySet()) {
            int nodeId = entry.getKey();
            Optional<BrokerRegistration> brokerRegistration = entry.getValue();
            if (!newBrokers.containsKey(nodeId)) {
                if (brokerRegistration.isPresent()) {
                    newBrokers.put(nodeId, brokerRegistration.get());
                }
            }
        }
        return new ClusterImage(newBrokers);
    }

    @Override
    public String toString() {
        return "ClusterDelta(" +
            "changedBrokers=" + changedBrokers +
            ')';
    }
}

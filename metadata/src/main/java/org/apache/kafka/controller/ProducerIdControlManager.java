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

package org.apache.kafka.controller;

import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.ProducerIdsBlock;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineLong;
import org.apache.kafka.timeline.TimelineObject;
import org.slf4j.Logger;

import java.util.Collections;


public class ProducerIdControlManager {
    static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private ClusterControlManager clusterControlManager = null;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder setClusterControlManager(ClusterControlManager clusterControlManager) {
            this.clusterControlManager = clusterControlManager;
            return this;
        }

        ProducerIdControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (clusterControlManager == null) {
                throw new RuntimeException("You must specify ClusterControlManager.");
            }
            return new ProducerIdControlManager(
                logContext,
                clusterControlManager,
                snapshotRegistry);
        }
    }

    private final Logger log;
    private final ClusterControlManager clusterControlManager;
    private final TimelineObject<ProducerIdsBlock> nextProducerBlock;
    private final TimelineLong brokerEpoch;

    private ProducerIdControlManager(
        LogContext logContext,
        ClusterControlManager clusterControlManager,
        SnapshotRegistry snapshotRegistry
    ) {
        this.log = logContext.logger(ProducerIdControlManager.class);
        this.clusterControlManager = clusterControlManager;
        this.nextProducerBlock = new TimelineObject<>(snapshotRegistry, ProducerIdsBlock.EMPTY);
        this.brokerEpoch = new TimelineLong(snapshotRegistry);
    }

    ControllerResult<ProducerIdsBlock> generateNextProducerId(int brokerId, long brokerEpoch) {
        clusterControlManager.checkBrokerEpoch(brokerId, brokerEpoch);

        long firstProducerIdInBlock = nextProducerBlock.get().firstProducerId();
        if (firstProducerIdInBlock > Long.MAX_VALUE - ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE) {
            throw new UnknownServerException("Exhausted all producerIds as the next block's end producerId " +
                "has exceeded the int64 type limit");
        }

        ProducerIdsBlock block = new ProducerIdsBlock(brokerId, firstProducerIdInBlock, ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE);
        long newNextProducerId = block.nextBlockFirstId();

        ProducerIdsRecord record = new ProducerIdsRecord()
            .setNextProducerId(newNextProducerId)
            .setBrokerId(brokerId)
            .setBrokerEpoch(brokerEpoch);
        return ControllerResult.of(Collections.singletonList(new ApiMessageAndVersion(record, (short) 0)), block);
    }

    // VisibleForTesting
    ProducerIdsBlock nextProducerBlock() {
        return nextProducerBlock.get();
    }

    void replay(ProducerIdsRecord record) {
        // During a migration, we may be calling replay() without ever having called generateNextProducerId(),
        // so the next producer block could be EMPTY
        ProducerIdsBlock nextBlock = nextProducerBlock.get();
        if (nextBlock != ProducerIdsBlock.EMPTY && record.nextProducerId() <= nextBlock.firstProducerId()) {
            throw new RuntimeException("Next Producer ID from replayed record (" + record.nextProducerId() + ")" +
                " is not greater than current next Producer ID in block (" + nextBlock + ")");
        } else {
            log.info("Replaying ProducerIdsRecord {}", record);
            nextProducerBlock.set(new ProducerIdsBlock(record.brokerId(), record.nextProducerId(),
                    ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE));
            brokerEpoch.set(record.brokerEpoch());
        }
    }
}

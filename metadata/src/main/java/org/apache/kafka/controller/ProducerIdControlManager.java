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
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.ProducerIdsBlock;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineLong;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


public class ProducerIdControlManager {

    private final ClusterControlManager clusterControlManager;
    private final TimelineLong nextProducerId; // Initializes to 0

    ProducerIdControlManager(ClusterControlManager clusterControlManager, SnapshotRegistry snapshotRegistry) {
        this.clusterControlManager = clusterControlManager;
        this.nextProducerId = new TimelineLong(snapshotRegistry);
    }

    ControllerResult<ProducerIdsBlock> generateNextProducerId(int brokerId, long brokerEpoch) {
        clusterControlManager.checkBrokerEpoch(brokerId, brokerEpoch);

        long firstProducerIdInBlock = nextProducerId.get();
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

    void replay(ProducerIdsRecord record) {
        long currentNextProducerId = nextProducerId.get();
        if (record.nextProducerId() <= currentNextProducerId) {
            throw new RuntimeException("Next Producer ID from replayed record (" + record.nextProducerId() + ")" +
                " is not greater than current next Producer ID (" + currentNextProducerId + ")");
        } else {
            nextProducerId.set(record.nextProducerId());
        }
    }

    Iterator<List<ApiMessageAndVersion>> iterator(long epoch) {
        List<ApiMessageAndVersion> records = new ArrayList<>(1);

        long producerId = nextProducerId.get(epoch);
        if (producerId > 0) {
            records.add(new ApiMessageAndVersion(
                new ProducerIdsRecord()
                    .setNextProducerId(producerId)
                    .setBrokerId(0)
                    .setBrokerEpoch(0L),
                (short) 0));
        }
        return Collections.singleton(records).iterator();
    }
}

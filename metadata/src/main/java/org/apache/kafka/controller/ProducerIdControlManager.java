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
    private final TimelineLong lastProducerId;

    ProducerIdControlManager(ClusterControlManager clusterControlManager, SnapshotRegistry snapshotRegistry) {
        this.clusterControlManager = clusterControlManager;
        this.lastProducerId = new TimelineLong(snapshotRegistry, 0L);
    }

    ControllerResult<ProducerIdsBlock> generateNextProducerId(int brokerId, long brokerEpoch) {
        clusterControlManager.checkBrokerEpoch(brokerId, brokerEpoch);

        long producerId = lastProducerId.get();

        if (producerId > Long.MAX_VALUE - ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE) {
            throw new UnknownServerException("Exhausted all producerIds as the next block's end producerId " +
                "is will has exceeded long type limit");
        }

        long nextProducerId = producerId + ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE;
        ProducerIdsRecord record = new ProducerIdsRecord()
            .setProducerIdsEnd(nextProducerId)
            .setBrokerId(brokerId)
            .setBrokerEpoch(brokerEpoch);
        ProducerIdsBlock block = new ProducerIdsBlock(brokerId, producerId, ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE);
        return ControllerResult.of(Collections.singletonList(new ApiMessageAndVersion(record, (short) 0)), block);
    }

    void replay(ProducerIdsRecord record) {
        long currentProducerId = lastProducerId.get();
        if (record.producerIdsEnd() <= currentProducerId) {
            throw new RuntimeException("Producer ID from record is not monotonically increasing");
        } else {
            lastProducerId.set(record.producerIdsEnd());
        }
    }

    Iterator<List<ApiMessageAndVersion>> iterator(long epoch) {
        List<ApiMessageAndVersion> records = new ArrayList<>(1);

        long producerId = lastProducerId.get(epoch);
        if (producerId > 0) {
            records.add(new ApiMessageAndVersion(
                new ProducerIdsRecord()
                    .setProducerIdsEnd(producerId)
                    .setBrokerId(0)
                    .setBrokerEpoch(0L),
                (short) 0));
        }
        return Collections.singleton(records).iterator();
    }
}

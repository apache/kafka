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

import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.ProducerIdsBlock;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.Errors.UNKNOWN_SERVER_ERROR;


public class ProducerIdControlManager {
    private static final Object PRODUCER_ID_KEY = new Object();
    private static final int PRODUCER_ID_BLOCK_SIZE = 1000;

    private final ClusterControlManager clusterControlManager;
    private final TimelineHashMap<Object, Long> lastProducerId;

    ProducerIdControlManager(ClusterControlManager clusterControlManager, SnapshotRegistry snapshotRegistry) {
        this.clusterControlManager = clusterControlManager;
        this.lastProducerId = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    ControllerResult<ResultOrError<ProducerIdsBlock>> generateNextProducerId(int brokerId, long brokerEpoch) {
        try {
            clusterControlManager.checkBrokerEpoch(brokerId, brokerEpoch);
        } catch (StaleBrokerEpochException e) {
            return ControllerResult.of(Collections.emptyList(), ResultOrError.of(ApiError.fromThrowable(e)));
        }

        long producerId = lastProducerId.getOrDefault(PRODUCER_ID_KEY, 0L);

        if (producerId > Long.MAX_VALUE - PRODUCER_ID_BLOCK_SIZE) {
            ApiError error = new ApiError(UNKNOWN_SERVER_ERROR,
                "Exhausted all producerIds as the next block's end producerId " +
                "is will has exceeded long type limit");
            return ControllerResult.of(Collections.emptyList(), ResultOrError.of(error));
        }

        long nextProducerId = producerId + PRODUCER_ID_BLOCK_SIZE;
        ProducerIdsRecord record = new ProducerIdsRecord()
            .setProducerIdsEnd(nextProducerId)
            .setBrokerId(brokerId)
            .setBrokerEpoch(brokerEpoch);
        ProducerIdsBlock block = new ProducerIdsBlock(-1, producerId, PRODUCER_ID_BLOCK_SIZE);
        return ControllerResult.of(
            Collections.singletonList(new ApiMessageAndVersion(record, (short) 0)), ResultOrError.of(block));
    }

    void replay(ProducerIdsRecord record) {
        long currentProducerId = lastProducerId.getOrDefault(PRODUCER_ID_KEY, 0L);
        if (record.producerIdsEnd() <= currentProducerId) {
            throw new RuntimeException("Producer ID from record is not monotonically increasing");
        } else {
            lastProducerId.put(PRODUCER_ID_KEY, record.producerIdsEnd());
        }
    }

    Iterator<List<ApiMessageAndVersion>> iterator(long epoch) {
        List<ApiMessageAndVersion> records = new ArrayList<>(1);

        long producerId = 0L;
        for (Map.Entry<Object, Long> entry : lastProducerId.entrySet(epoch)) {
            if (entry.getKey() == PRODUCER_ID_KEY) {
                producerId = lastProducerId.getOrDefault(PRODUCER_ID_KEY, 0L);
            } else {
                throw new IllegalStateException("Unexpected key in producer ids map " + entry.getKey());
            }
        }
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

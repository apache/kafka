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
package org.apache.kafka.trogdor.workload;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.trogdor.common.JsonUtil;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class acknowledges each record with a randomly chosen type. The percentage of records that are rejected and
 * released is configurable, and all the remaining records are accepted. The number of records acknowledged with each
 * type is present in the status from the `ShareConsumeBenchWorker` class.
 *
 * Example spec:
 * {
 *    "type": "random",
 *    "rejectPercent": 20,
 *    "releasePercent": 10
 * }
 *
 * This will reject 20% of the records, release 10% of them and accept the remaining 70%.
 */
public class RandomRecordAcknowledger implements RecordAcknowledger {
    private final int rejectPercent;
    private final int releasePercent;
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong released = new AtomicLong();

    @JsonCreator
    public RandomRecordAcknowledger(@JsonProperty("rejectPercent") int rejectPercent,
                                    @JsonProperty("releasePercent") int releasePercent) {
        if (rejectPercent < 0 || releasePercent < 0 || rejectPercent + releasePercent > 100) {
            throw new IllegalArgumentException(String.format(
                    "rejectPercent and releasePercent must be non-negative and sum to at most 100, but were %d and %d",
                    rejectPercent, releasePercent));
        }
        this.rejectPercent = rejectPercent;
        this.releasePercent = releasePercent;
    }

    @JsonProperty
    public int rejectPercent() {
        return rejectPercent;
    }

    @JsonProperty
    public int releasePercent() {
        return releasePercent;
    }

    @Override
    public AcknowledgeType acknowledge(ConsumerRecord<byte[], byte[]> record) {
        int roll = ThreadLocalRandom.current().nextInt(100);
        if (roll < rejectPercent) {
            rejected.incrementAndGet();
            return AcknowledgeType.REJECT;
        } else if (roll < rejectPercent + releasePercent) {
            released.incrementAndGet();
            return AcknowledgeType.RELEASE;
        }
        accepted.incrementAndGet();
        return AcknowledgeType.ACCEPT;
    }

    @Override
    public JsonNode acknowledgerStatus() {
        Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("accepted", accepted.get());
        counts.put("rejected", rejected.get());
        counts.put("released", released.get());
        return JsonUtil.JSON_SERDE.valueToTree(counts);
    }
}

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

import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

// TODO figure out if we can find a more friendly form for this class
public class LogLengthInfoStore {
    private final Map<TopicIdPartition, Map<Integer, EpochOffset>> store;

    public LogLengthInfoStore(int expectedSize) {
        this.store = new HashMap<>(expectedSize);
    }

    public void add(TopicIdPartition tp, int replica, EpochOffset eo) {
        if (store.containsKey(tp)) {
            store.get(tp).put(replica, eo);
        } else {
            Map<Integer, EpochOffset> map = new HashMap<>();
            map.put(replica, eo);
            store.put(tp, map);
        }
    }

    public Set<TopicIdPartition> topics() {
        return store.keySet();
    }

    public Map<Integer, EpochOffset> get(TopicIdPartition tp) {
        return store.get(tp);
    }

    public static class EpochOffset implements Comparable<EpochOffset> {
        public final int epoch;
        public final long offset;

        EpochOffset(int epoch, long offset) {
            this.epoch = epoch;
            this.offset = offset;
        }

        public static final EpochOffset MIN = new EpochOffset(Integer.MIN_VALUE, Long.MIN_VALUE);
        public static final EpochOffset MAX = new EpochOffset(Integer.MAX_VALUE, Long.MAX_VALUE);

        @Override
        public int compareTo(EpochOffset o) {
            if (this.epoch == o.epoch) {
                if (this.offset == o.offset) {
                    return 0;
                }
                return this.offset < o.offset ? -1 : 1;
            }
            return this.epoch - o.epoch;
        }

        public static EpochOffset from(GetReplicaLogInfoResponseData.PartitionLogInfo info) {
            return new EpochOffset(info.lastWrittenLeaderEpoch(), info.logEndOffset());
        }
    }
}

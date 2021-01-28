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

package org.apache.kafka.timeline;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * A snapshot of some timeline data structures.
 *
 * The snapshot contains historical data for several timeline data structures.
 * We use an IdentityHashMap to store this data.  This way, removing the snapshot from
 * the snapshot registry deletes the data for all the structures simultaneously, in
 * O(1) time.
 */
class Snapshot {
    private final long epoch;
    private final IdentityHashMap<Revertable, Object> map = new IdentityHashMap<>(4);

    Snapshot(long epoch) {
        this.epoch = epoch;
    }

    long epoch() {
        return epoch;
    }

    @SuppressWarnings("unchecked")
    <T> T data(Revertable owner) {
        return (T) map.get(owner);
    }

    <T> void setData(Revertable owner, T data) {
        map.put(owner, data);
    }

    void handleRevert() {
        for (Map.Entry<Revertable, Object> entry : map.entrySet()) {
            entry.getKey().executeRevert(epoch, entry.getValue());
        }
    }
}

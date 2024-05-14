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

import org.apache.kafka.common.utils.LogContext;

import java.util.ArrayList;
import java.util.List;


public class TrackingSnapshotRegistry extends SnapshotRegistry {
    private final List<String> operations = new ArrayList<>();

    public TrackingSnapshotRegistry(LogContext logContext) {
        super(logContext);
    }

    public List<String> operations() {
        return new ArrayList<>(operations);
    }

    @Override
    public void revertToSnapshot(long targetEpoch) {
        operations.add("revert[" + targetEpoch + "]");
        super.revertToSnapshot(targetEpoch);
    }

    @Override
    public void reset() {
        operations.add("reset");
        super.reset();
    }

    @Override
    public Snapshot getOrCreateSnapshot(long epoch) {
        if (!hasSnapshot(epoch)) {
            operations.add("snapshot[" + epoch + "]");
        }
        return super.getOrCreateSnapshot(epoch);
    }
}

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
package org.apache.kafka.raft;

import java.util.Collections;
import java.util.Optional;

public class MockQuorumStateStore implements QuorumStateStore {
    private ElectionState current;

    @Override
    public ElectionState readElectionState() {
        return current;
    }

    @Override
    public void writeElectionState(ElectionState update, short kraftVersion) {
        if (kraftVersion == 0) {
            // kraft.version 0 doesn't support votedUuid
            this.current = new ElectionState(
                update.epoch(),
                update.optionalLeaderId(),
                update.optionalVotedId(),
                Optional.empty(),
                update.voters()
            );
        } else if (kraftVersion == 1) {
            // kraft.version 1 doesn't support voters
            this.current = new ElectionState(
                update.epoch(),
                update.optionalLeaderId(),
                update.optionalVotedId(),
                update.votedUuid(),
                Collections.emptySet()
            );
        } else {
            throw new IllegalArgumentException(
                String.format("Unknown kraft.version %d", kraftVersion)
            );
        }
    }

    @Override
    public void clear() {
        current = null;
    }
}

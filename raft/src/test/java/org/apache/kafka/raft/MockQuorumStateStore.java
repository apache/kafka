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

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.kafka.raft.generated.QuorumStateData;

public class MockQuorumStateStore implements QuorumStateStore {
    private Optional<QuorumStateData> current = Optional.empty();

    @Override
    public Optional<ElectionState> readElectionState() {
        return current.map(ElectionState::fromQuorumStateData);
    }

    @Override
    public void writeElectionState(ElectionState update, short kraftVersion) {
        current = Optional.of(
            update.toQuorumStateData(quorumStateVersionFromKRaftVersion(kraftVersion))
        );
    }

    @Override
    public Path path() {
        return FileSystems.getDefault().getPath("mock-file");
    }

    @Override
    public void clear() {
        current = Optional.empty();
    }

    private short quorumStateVersionFromKRaftVersion(short kraftVersion) {
        if (kraftVersion == 0) {
            return 0;
        } else if (kraftVersion == 1) {
            return 1;
        } else {
            throw new IllegalArgumentException(
                String.format("Unknown kraft.version %d", kraftVersion)
            );
        }
    }
}

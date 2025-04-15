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
package org.apache.kafka.raft.internals;

import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.server.common.KRaftVersion;

import java.util.Optional;

// TODO: Document this
public sealed interface KRaftVersionUpgrade {
    public record Empty() implements KRaftVersionUpgrade {
    }

    public record Version(KRaftVersion kraftVersion) implements KRaftVersionUpgrade {
    }

    public record Voters(VoterSet voters) implements KRaftVersionUpgrade {
    }

    public default Optional<Voters> toVoters() {
        if (this instanceof Voters) {
            return Optional.of(((Voters) this));
        } else {
            return Optional.empty();
        }
    }

    public default Optional<Version> toVersion() {
        if (this instanceof Version) {
            return Optional.of(((Version) this));
        } else {
            return Optional.empty();
        }
    }

    static final KRaftVersionUpgrade EMPTY = new Empty();

    public static KRaftVersionUpgrade empty() {
        return EMPTY;
    }

}

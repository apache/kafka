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

package org.apache.kafka.metalog;

import java.util.Objects;

/**
 * The current leader of the MetaLog.
 */
public class MetaLogLeader {
    private final int nodeId;
    private final long epoch;

    public MetaLogLeader(int nodeId, long epoch) {
        this.nodeId = nodeId;
        this.epoch = epoch;
    }

    public int nodeId() {
        return nodeId;
    }

    public long epoch() {
        return epoch;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MetaLogLeader)) return false;
        MetaLogLeader other = (MetaLogLeader) o;
        return other.nodeId == nodeId && other.epoch == epoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, epoch);
    }

    @Override
    public String toString() {
        return "MetaLogLeader(nodeId=" + nodeId + ", epoch=" + epoch + ")";
    }
}

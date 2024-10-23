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

package kafka.server.share;

import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;

import java.util.Objects;

public class FetchPartitionOffsetData {
    private final FetchPartitionData fetchPartitionData;
    private final LogOffsetMetadata logOffsetMetadata;

    public FetchPartitionOffsetData(FetchPartitionData fetchPartitionData, LogOffsetMetadata logOffsetMetadata) {
        this.fetchPartitionData = fetchPartitionData;
        this.logOffsetMetadata = logOffsetMetadata;
    }

    public FetchPartitionData fetchPartitionData() {
        return fetchPartitionData;
    }

    public LogOffsetMetadata logOffsetMetadata() {
        return logOffsetMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FetchPartitionOffsetData that = (FetchPartitionOffsetData) o;
        return fetchPartitionData.equals(that.fetchPartitionData) && logOffsetMetadata == that.logOffsetMetadata;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fetchPartitionData, logOffsetMetadata);
    }

    @Override
    public String toString() {
        return "FetchPartitionOffsetData(fetchPartitionData=" + fetchPartitionData +
            ", logOffsetMetadata=" + logOffsetMetadata + ")";
    }
}

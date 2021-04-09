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
package org.apache.kafka.clients.admin;

/**
 * A description of a replica on a particular broker.
 */
public class ReplicaInfo {

    private final long size;
    private final long offsetLag;
    private final boolean isFuture;

    public ReplicaInfo(long size, long offsetLag, boolean isFuture) {
        this.size = size;
        this.offsetLag = offsetLag;
        this.isFuture = isFuture;
    }

    /**
     * The total size of the log segments in this replica in bytes.
     */
    public long size() {
        return size;
    }

    /**
     * The lag of the log's LEO with respect to the partition's
     * high watermark (if it is the current log for the partition)
     * or the current replica's LEO (if it is the {@linkplain #isFuture() future log}
     * for the partition).
     */
    public long offsetLag() {
        return offsetLag;
    }

    /**
     * Whether this replica has been created by a AlterReplicaLogDirsRequest
     * but not yet replaced the current replica on the broker.
     *
     * @return true if this log is created by AlterReplicaLogDirsRequest and will replace the current log
     * of the replica at some time in the future.
     */
    public boolean isFuture() {
        return isFuture;
    }

    @Override
    public String toString() {
        return "ReplicaInfo(" +
                "size=" + size +
                ", offsetLag=" + offsetLag +
                ", isFuture=" + isFuture +
                ')';
    }
}

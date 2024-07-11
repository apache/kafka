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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.protocol.MessageUtil;

import java.util.ArrayList;
import java.util.List;

public class AcknowledgementBatch {
    private long firstOffset;
    private long lastOffset;
    private List<Byte> acknowledgeTypes;

    public AcknowledgementBatch() {
        this.firstOffset = 0L;
        this.lastOffset = 0L;
        this.acknowledgeTypes = new ArrayList<>(0);
    }

    public long firstOffset() {
        return this.firstOffset;
    }

    public long lastOffset() {
        return this.lastOffset;
    }

    public List<Byte> acknowledgeTypes() {
        return this.acknowledgeTypes;
    }

    public AcknowledgementBatch setFirstOffset(long v) {
        this.firstOffset = v;
        return this;
    }

    public AcknowledgementBatch setLastOffset(long v) {
        this.lastOffset = v;
        return this;
    }

    public AcknowledgementBatch setAcknowledgeTypes(List<Byte> v) {
        this.acknowledgeTypes = v;
        return this;
    }

    public ShareAcknowledgeRequestData.AcknowledgementBatch toShareAcknowledgeRequest() {
        return new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(firstOffset)
                .setLastOffset(lastOffset)
                .setAcknowledgeTypes(acknowledgeTypes);
    }

    public ShareFetchRequestData.AcknowledgementBatch toShareFetchRequest() {
        return new ShareFetchRequestData.AcknowledgementBatch()
                .setFirstOffset(firstOffset)
                .setLastOffset(lastOffset)
                .setAcknowledgeTypes(acknowledgeTypes);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AcknowledgementBatch)) return false;
        AcknowledgementBatch other = (AcknowledgementBatch) obj;
        if (firstOffset != other.firstOffset) return false;
        if (lastOffset != other.lastOffset) return false;
        if (this.acknowledgeTypes == null) {
            return other.acknowledgeTypes == null;
        } else {
            return this.acknowledgeTypes.equals(other.acknowledgeTypes);
        }
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + ((int) (firstOffset >> 32) ^ (int) firstOffset);
        hashCode = 31 * hashCode + ((int) (lastOffset >> 32) ^ (int) lastOffset);
        hashCode = 31 * hashCode + (acknowledgeTypes == null ? 0 : acknowledgeTypes.hashCode());
        return hashCode;
    }

    @Override
    public String toString() {
        return "AcknowledgementBatch("
                + "firstOffset=" + firstOffset
                + ", lastOffset=" + lastOffset
                + ", acknowledgeTypes=" + MessageUtil.deepToString(acknowledgeTypes.iterator())
                + ")";
    }
}

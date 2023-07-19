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

package org.apache.kafka.image;

import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.image.node.ProducerIdsImageNode;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;

import java.util.Objects;


/**
 * Stores the highest seen producer ID in the metadata image.
 *
 * This class is thread-safe.
 */
public final class ProducerIdsImage {
    public final static ProducerIdsImage EMPTY = new ProducerIdsImage(-1L);

    /**
     * The next producer ID, or -1 in the special case where no producer IDs have been issued.
     */
    private final long nextProducerId;

    public ProducerIdsImage(long nextProducerId) {
        this.nextProducerId = nextProducerId;
    }

    public long nextProducerId() {
        return nextProducerId;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        if (nextProducerId >= 0) {
            writer.write(0, new ProducerIdsRecord().
                    setBrokerId(-1).
                    setBrokerEpoch(-1).
                    setNextProducerId(nextProducerId));
        }
    }

    public boolean isEmpty() {
        return nextProducerId == EMPTY.nextProducerId;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ProducerIdsImage)) return false;
        ProducerIdsImage other = (ProducerIdsImage) o;
        return nextProducerId == other.nextProducerId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nextProducerId);
    }

    @Override
    public String toString() {
        return new ProducerIdsImageNode(this).stringify();
    }
}

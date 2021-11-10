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

package org.apache.kafka.server.common;

import java.util.Objects;

/**
 * Holds a range of Producer IDs used for Transactional and EOS producers.
 *
 * The start and end of the ID block are inclusive.
 */
public class ProducerIdsBlock {
    public static final int PRODUCER_ID_BLOCK_SIZE = 1000;

    public static final ProducerIdsBlock EMPTY = new ProducerIdsBlock(-1, 0, 0);

    private final int brokerId;
    private final long producerIdStart;
    private final int producerIdLen;

    public ProducerIdsBlock(int brokerId, long producerIdStart, int producerIdLen) {
        this.brokerId = brokerId;
        this.producerIdStart = producerIdStart;
        this.producerIdLen = producerIdLen;
    }

    public int brokerId() {
        return brokerId;
    }

    public long producerIdStart() {
        return producerIdStart;
    }

    public int producerIdLen() {
        return producerIdLen;
    }

    public long producerIdEnd() {
        return producerIdStart + producerIdLen - 1;
    }


    @Override
    public String toString() {
        return "ProducerIdsBlock{" +
                "brokerId=" + brokerId +
                ", producerIdStart=" + producerIdStart +
                ", producerIdLen=" + producerIdLen +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProducerIdsBlock that = (ProducerIdsBlock) o;
        return brokerId == that.brokerId && producerIdStart == that.producerIdStart && producerIdLen == that.producerIdLen;
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId, producerIdStart, producerIdLen);
    }
}

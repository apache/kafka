package org.apache.kafka.server.common;

import java.util.Objects;

/**
 * Holds a range of Producer IDs used for Transactional and EOS producers.
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

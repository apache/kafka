package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class RecordBatchTest {

    /**
     * A RecordBatch configured using very large linger value, in combination
     * with a timestamp preceding the create time of a RecordBatch, is
     * interpreted correctly as not expired when the linger time is larger than
     * the difference between now and create time by RecordBatch#maybeExpire.
     */
    @Test
    public void testLargeLingerOldNowExpire() {
        final long now = 1488748346917L;
        final RecordBatch batch = new RecordBatch(
            new TopicPartition("topic", 1),
            new MemoryRecordsBuilder(
                ByteBuffer.allocate(0), Record.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L, Record.NO_TIMESTAMP, 0
            ), now
        );
        // Use `now` 2ms before the create time.
        assertFalse(
            batch.maybeExpire(10240, 100L, now - 2L, Long.MAX_VALUE, false)
        );
    }
}

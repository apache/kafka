package kafka.clients.producer;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import kafka.clients.producer.internals.RecordAccumulator;
import kafka.clients.producer.internals.RecordBatch;
import kafka.common.TopicPartition;
import kafka.common.metrics.Metrics;
import kafka.common.record.CompressionType;
import kafka.common.record.LogEntry;
import kafka.common.record.Record;
import kafka.common.record.Records;
import kafka.common.utils.MockTime;

import org.junit.Test;

public class RecordAccumulatorTest {

    private TopicPartition tp = new TopicPartition("test", 0);
    private MockTime time = new MockTime();
    private byte[] key = "key".getBytes();
    private byte[] value = "value".getBytes();
    private int msgSize = Records.LOG_OVERHEAD + Record.recordSize(key, value);
    private Metrics metrics = new Metrics(time);

    @Test
    public void testFull() throws Exception {
        long now = time.milliseconds();
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, 10L, false, metrics, time);
        int appends = 1024 / msgSize;
        for (int i = 0; i < appends; i++) {
            accum.append(tp, key, value, CompressionType.NONE, null);
            assertEquals("No partitions should be ready.", 0, accum.ready(now).size());
        }
        accum.append(tp, key, value, CompressionType.NONE, null);
        assertEquals("Our partition should be ready", asList(tp), accum.ready(time.milliseconds()));
        List<RecordBatch> batches = accum.drain(asList(tp), Integer.MAX_VALUE);
        assertEquals(1, batches.size());
        RecordBatch batch = batches.get(0);
        Iterator<LogEntry> iter = batch.records.iterator();
        for (int i = 0; i < appends; i++) {
            LogEntry entry = iter.next();
            assertEquals("Keys should match", ByteBuffer.wrap(key), entry.record().key());
            assertEquals("Values should match", ByteBuffer.wrap(value), entry.record().value());
        }
        assertFalse("No more records", iter.hasNext());
    }

    @Test
    public void testAppendLarge() throws Exception {
        int batchSize = 512;
        RecordAccumulator accum = new RecordAccumulator(batchSize, 10 * 1024, 0L, false, metrics, time);
        accum.append(tp, key, new byte[2 * batchSize], CompressionType.NONE, null);
        assertEquals("Our partition should be ready", asList(tp), accum.ready(time.milliseconds()));
    }

    @Test
    public void testLinger() throws Exception {
        long lingerMs = 10L;
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, lingerMs, false, metrics, time);
        accum.append(tp, key, value, CompressionType.NONE, null);
        assertEquals("No partitions should be ready", 0, accum.ready(time.milliseconds()).size());
        time.sleep(10);
        assertEquals("Our partition should be ready", asList(tp), accum.ready(time.milliseconds()));
        List<RecordBatch> batches = accum.drain(asList(tp), Integer.MAX_VALUE);
        assertEquals(1, batches.size());
        RecordBatch batch = batches.get(0);
        Iterator<LogEntry> iter = batch.records.iterator();
        LogEntry entry = iter.next();
        assertEquals("Keys should match", ByteBuffer.wrap(key), entry.record().key());
        assertEquals("Values should match", ByteBuffer.wrap(value), entry.record().value());
        assertFalse("No more records", iter.hasNext());
    }

    @Test
    public void testPartialDrain() throws Exception {
        RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, 10L, false, metrics, time);
        int appends = 1024 / msgSize + 1;
        List<TopicPartition> partitions = asList(new TopicPartition("test", 0), new TopicPartition("test", 1));
        for (TopicPartition tp : partitions) {
            for (int i = 0; i < appends; i++)
                accum.append(tp, key, value, CompressionType.NONE, null);
        }
        assertEquals("Both partitions should be ready", 2, accum.ready(time.milliseconds()).size());

        List<RecordBatch> batches = accum.drain(partitions, 1024);
        assertEquals("But due to size bound only one partition should have been retrieved", 1, batches.size());
    }

    @Test
    public void testStressfulSituation() throws Exception {
        final int numThreads = 5;
        final int msgs = 10000;
        final int numParts = 10;
        final RecordAccumulator accum = new RecordAccumulator(1024, 10 * 1024, 0L, true, metrics, time);
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < numThreads; i++) {
            threads.add(new Thread() {
                public void run() {
                    for (int i = 0; i < msgs; i++) {
                        try {
                            accum.append(new TopicPartition("test", i % numParts), key, value, CompressionType.NONE, null);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        for (Thread t : threads)
            t.start();
        int read = 0;
        long now = time.milliseconds();
        while (read < numThreads * msgs) {
            List<TopicPartition> tps = accum.ready(now);
            List<RecordBatch> batches = accum.drain(tps, 5 * 1024);
            for (RecordBatch batch : batches) {
                for (LogEntry entry : batch.records)
                    read++;
            }
            accum.deallocate(batches);
        }

        for (Thread t : threads)
            t.join();
    }

}

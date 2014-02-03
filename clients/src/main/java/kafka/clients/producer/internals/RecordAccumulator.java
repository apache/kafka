package kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import kafka.clients.producer.Callback;
import kafka.common.TopicPartition;
import kafka.common.metrics.Measurable;
import kafka.common.metrics.MetricConfig;
import kafka.common.metrics.Metrics;
import kafka.common.record.CompressionType;
import kafka.common.record.MemoryRecords;
import kafka.common.record.Record;
import kafka.common.record.Records;
import kafka.common.utils.CopyOnWriteMap;
import kafka.common.utils.Time;
import kafka.common.utils.Utils;

/**
 * This class acts as a queue that accumulates records into {@link kafka.common.record.MemoryRecords} instances to be
 * sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 */
public final class RecordAccumulator {

    private volatile boolean closed;
    private int drainIndex;
    private final int batchSize;
    private final long lingerMs;
    private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;
    private final BufferPool free;
    private final Time time;

    /**
     * Create a new record accumulator
     * 
     * @param batchSize The size to use when allocating {@link kafka.common.record.MemoryRecords} instances
     * @param totalSize The maximum memory the record accumulator can use.
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param blockOnBufferFull If true block when we are out of memory; if false throw an exception when we are out of
     *        memory
     * @param metrics The metrics
     * @param time The time instance to use
     */
    public RecordAccumulator(int batchSize, long totalSize, long lingerMs, boolean blockOnBufferFull, Metrics metrics, Time time) {
        this.drainIndex = 0;
        this.closed = false;
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.batches = new CopyOnWriteMap<TopicPartition, Deque<RecordBatch>>();
        this.free = new BufferPool(totalSize, batchSize, blockOnBufferFull);
        this.time = time;
        registerMetrics(metrics);
    }

    private void registerMetrics(Metrics metrics) {
        metrics.addMetric("blocked_threads",
                          "The number of user threads blocked waiting for buffer memory to enqueue their records",
                          new Measurable() {
                              public double measure(MetricConfig config, long now) {
                                  return free.queued();
                              }
                          });
        metrics.addMetric("buffer_total_bytes",
                          "The total amount of buffer memory that is available (not currently used for buffering records).",
                          new Measurable() {
                              public double measure(MetricConfig config, long now) {
                                  return free.totalMemory();
                              }
                          });
        metrics.addMetric("buffer_available_bytes",
                          "The total amount of buffer memory that is available (not currently used for buffering records).",
                          new Measurable() {
                              public double measure(MetricConfig config, long now) {
                                  return free.availableMemory();
                              }
                          });
    }

    /**
     * Add a record to the accumulator.
     * <p>
     * This method will block if sufficient memory isn't available for the record unless blocking has been disabled.
     * 
     * @param tp The topic/partition to which this record is being sent
     * @param key The key for the record
     * @param value The value for the record
     * @param compression The compression codec for the record
     * @param callback The user-supplied callback to execute when the request is complete
     */
    public FutureRecordMetadata append(TopicPartition tp, byte[] key, byte[] value, CompressionType compression, Callback callback) throws InterruptedException {
        if (closed)
            throw new IllegalStateException("Cannot send after the producer is closed.");
        // check if we have an in-progress batch
        Deque<RecordBatch> dq = dequeFor(tp);
        synchronized (dq) {
            RecordBatch batch = dq.peekLast();
            if (batch != null) {
                FutureRecordMetadata future = batch.tryAppend(key, value, compression, callback);
                if (future != null)
                    return future;
            }
        }

        // we don't have an in-progress record batch try to allocate a new batch
        int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
        ByteBuffer buffer = free.allocate(size);
        synchronized (dq) {
            RecordBatch first = dq.peekLast();
            if (first != null) {
                FutureRecordMetadata future = first.tryAppend(key, value, compression, callback);
                if (future != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen
                    // often...
                    free.deallocate(buffer);
                    return future;
                }
            }
            RecordBatch batch = new RecordBatch(tp, new MemoryRecords(buffer), time.milliseconds());
            FutureRecordMetadata future = Utils.notNull(batch.tryAppend(key, value, compression, callback));
            dq.addLast(batch);
            return future;
        }
    }

    /**
     * Get a list of topic-partitions which are ready to be sent.
     * <p>
     * A partition is ready if ANY of the following are true:
     * <ol>
     * <li>The record set is full
     * <li>The record set has sat in the accumulator for at least lingerMs milliseconds
     * <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions are
     * immediately considered ready).
     * <li>The accumulator has been closed
     * </ol>
     */
    public List<TopicPartition> ready(long now) {
        List<TopicPartition> ready = new ArrayList<TopicPartition>();
        boolean exhausted = this.free.queued() > 0;
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> deque = entry.getValue();
            synchronized (deque) {
                RecordBatch batch = deque.peekFirst();
                if (batch != null) {
                    boolean full = deque.size() > 1 || !batch.records.buffer().hasRemaining();
                    boolean expired = now - batch.created >= lingerMs;
                    if (full | expired | exhausted | closed)
                        ready.add(batch.topicPartition);
                }
            }
        }
        return ready;
    }

    /**
     * Drain all the data for the given topic-partitions that will fit within the specified size. This method attempts
     * to avoid choosing the same topic-partitions over and over.
     * 
     * @param partitions The list of partitions to drain
     * @param maxSize The maximum number of bytes to drain
     * @return A list of {@link RecordBatch} for partitions specified with total size less than the requested maxSize.
     *         TODO: There may be a starvation issue due to iteration order
     */
    public List<RecordBatch> drain(List<TopicPartition> partitions, int maxSize) {
        if (partitions.isEmpty())
            return Collections.emptyList();
        int size = 0;
        List<RecordBatch> ready = new ArrayList<RecordBatch>();
        /* to make starvation less likely this loop doesn't start at 0 */
        int start = drainIndex = drainIndex % partitions.size();
        do {
            TopicPartition tp = partitions.get(drainIndex);
            Deque<RecordBatch> deque = dequeFor(tp);
            if (deque != null) {
                synchronized (deque) {
                    if (size + deque.peekFirst().records.sizeInBytes() > maxSize) {
                        return ready;
                    } else {
                        RecordBatch batch = deque.pollFirst();
                        size += batch.records.sizeInBytes();
                        ready.add(batch);
                    }
                }
            }
            this.drainIndex = (this.drainIndex + 1) % partitions.size();
        } while (start != drainIndex);
        return ready;
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary. Since new topics will only be added rarely
     * we copy-on-write the hashmap
     */
    private Deque<RecordBatch> dequeFor(TopicPartition tp) {
        Deque<RecordBatch> d = this.batches.get(tp);
        if (d != null)
            return d;
        this.batches.putIfAbsent(tp, new ArrayDeque<RecordBatch>());
        return this.batches.get(tp);
    }

    /**
     * Deallocate the list of record batches
     */
    public void deallocate(Collection<RecordBatch> batches) {
        ByteBuffer[] buffers = new ByteBuffer[batches.size()];
        int i = 0;
        for (RecordBatch batch : batches) {
            buffers[i] = batch.records.buffer();
            i++;
        }
        free.deallocate(buffers);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
    }

}

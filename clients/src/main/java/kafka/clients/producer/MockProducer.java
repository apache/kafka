package kafka.clients.producer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.clients.producer.internals.ProduceRequestResult;
import kafka.common.Cluster;
import kafka.common.Metric;
import kafka.common.Serializer;
import kafka.common.TopicPartition;

/**
 * A mock of the producer interface you can use for testing code that uses Kafka.
 * <p>
 * By default this mock will synchronously complete each send call successfully. However it can be configured to allow
 * the user to control the completion of the call and supply an optional error for the producer to throw.
 */
public class MockProducer implements Producer {

    private final Serializer keySerializer;
    private final Serializer valueSerializer;
    private final Partitioner partitioner;
    private final Cluster cluster;
    private final List<ProducerRecord> sent;
    private final Deque<Completion> completions;
    private boolean autoComplete;
    private Map<TopicPartition, Long> offsets;

    /**
     * Create a mock producer
     * 
     * @param keySerializer A serializer to use on keys (useful to test your serializer on the values)
     * @param valueSerializer A serializer to use on values (useful to test your serializer on the values)
     * @param partitioner A partitioner to choose partitions (if null the partition will always be 0)
     * @param cluster The cluster to pass to the partitioner (can be null if partitioner is null)
     * @param autoComplete If true automatically complete all requests successfully and execute the callback. Otherwise
     *        the user must call {@link #completeNext()} or {@link #errorNext(RuntimeException)} after
     *        {@link #send(ProducerRecord) send()} to complete the call and unblock the @{link RecordSend} that is
     *        returned.
     */
    public MockProducer(Serializer keySerializer, Serializer valueSerializer, Partitioner partitioner, Cluster cluster, boolean autoComplete) {
        if (partitioner != null && (cluster == null | keySerializer == null | valueSerializer == null))
            throw new IllegalArgumentException("If a partitioner is provided a cluster instance and key and value serializer for partitioning must also be given.");
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.partitioner = partitioner;
        this.cluster = cluster;
        this.autoComplete = autoComplete;
        this.offsets = new HashMap<TopicPartition, Long>();
        this.sent = new ArrayList<ProducerRecord>();
        this.completions = new ArrayDeque<Completion>();
    }

    /**
     * Create a new mock producer with no serializers or partitioner and the given autoComplete setting.
     * 
     * Equivalent to {@link #MockProducer(Serializer, Serializer, Partitioner, Cluster, boolean) new MockProducer(null,
     * null, null, null, autoComplete)}
     */
    public MockProducer(boolean autoComplete) {
        this(null, null, null, null, autoComplete);
    }

    /**
     * Create a new auto completing mock producer with no serializers or partitioner.
     * 
     * Equivalent to {@link #MockProducer(boolean) new MockProducer(true)}
     */
    public MockProducer() {
        this(true);
    }

    /**
     * Adds the record to the list of sent records. The {@link RecordSend} returned will be immediately satisfied.
     * 
     * @see #history()
     */
    @Override
    public synchronized RecordSend send(ProducerRecord record) {
        return send(record, null);
    }

    /**
     * Adds the record to the list of sent records. The {@link RecordSend} returned will be immediately satisfied and
     * the callback will be synchronously executed.
     * 
     * @see #history()
     */
    @Override
    public synchronized RecordSend send(ProducerRecord record, Callback callback) {
        byte[] key = keySerializer == null ? null : keySerializer.toBytes(record.key());
        byte[] partitionKey = keySerializer == null ? null : keySerializer.toBytes(record.partitionKey());
        byte[] value = valueSerializer == null ? null : valueSerializer.toBytes(record.value());
        int numPartitions = partitioner == null ? 0 : this.cluster.partitionsFor(record.topic()).size();
        int partition = partitioner == null ? 0 : partitioner.partition(record, key, partitionKey, value, this.cluster, numPartitions);
        ProduceRequestResult result = new ProduceRequestResult();
        RecordSend send = new RecordSend(0, result);
        TopicPartition topicPartition = new TopicPartition(record.topic(), partition);
        long offset = nextOffset(topicPartition);
        Completion completion = new Completion(topicPartition, offset, send, result, callback);
        this.sent.add(record);
        if (autoComplete)
            completion.complete(null);
        else
            this.completions.addLast(completion);
        return send;
    }

    /**
     * Get the next offset for this topic/partition
     */
    private long nextOffset(TopicPartition tp) {
        Long offset = this.offsets.get(tp);
        if (offset == null) {
            this.offsets.put(tp, 1L);
            return 0L;
        } else {
            Long next = offset + 1;
            this.offsets.put(tp, next);
            return offset;
        }
    }

    public Map<String, Metric> metrics() {
        return Collections.emptyMap();
    }

    /**
     * "Closes" the producer
     */
    @Override
    public void close() {
    }

    /**
     * Get the list of sent records since the last call to {@link #clear()}
     */
    public synchronized List<ProducerRecord> history() {
        return new ArrayList<ProducerRecord>(this.sent);
    }

    /**
     * Clear the stored history of sent records
     */
    public synchronized void clear() {
        this.sent.clear();
        this.completions.clear();
    }

    /**
     * Complete the earliest uncompleted call successfully.
     * 
     * @return true if there was an uncompleted call to complete
     */
    public synchronized boolean completeNext() {
        return errorNext(null);
    }

    /**
     * Complete the earliest uncompleted call with the given error.
     * 
     * @return true if there was an uncompleted call to complete
     */
    public synchronized boolean errorNext(RuntimeException e) {
        Completion completion = this.completions.pollFirst();
        if (completion != null) {
            completion.complete(e);
            return true;
        } else {
            return false;
        }
    }

    private static class Completion {
        private final long offset;
        private final RecordSend send;
        private final ProduceRequestResult result;
        private final Callback callback;
        private final TopicPartition topicPartition;

        public Completion(TopicPartition topicPartition, long offset, RecordSend send, ProduceRequestResult result, Callback callback) {
            this.send = send;
            this.offset = offset;
            this.result = result;
            this.callback = callback;
            this.topicPartition = topicPartition;
        }

        public void complete(RuntimeException e) {
            result.done(topicPartition, e == null ? offset : -1L, e);
            if (callback != null)
                callback.onCompletion(send);
        }
    }

}

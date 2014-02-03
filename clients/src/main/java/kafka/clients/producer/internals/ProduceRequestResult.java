package kafka.clients.producer.internals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import kafka.clients.producer.RecordMetadata;
import kafka.common.TopicPartition;

/**
 * A class that models the future completion of a produce request for a single partition. There is one of these per
 * partition in a produce request and it is shared by all the {@link RecordMetadata} instances that are batched together
 * for the same partition in the request.
 */
public final class ProduceRequestResult {

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile TopicPartition topicPartition;
    private volatile long baseOffset = -1L;
    private volatile RuntimeException error;

    public ProduceRequestResult() {
    }

    /**
     * Mark this request as complete and unblock any threads waiting on its completion.
     * @param topicPartition The topic and partition to which this record set was sent was sent
     * @param baseOffset The base offset assigned to the record
     * @param error The error that occurred if there was one, or null.
     */
    public void done(TopicPartition topicPartition, long baseOffset, RuntimeException error) {
        this.topicPartition = topicPartition;
        this.baseOffset = baseOffset;
        this.error = error;
        this.latch.countDown();
    }

    /**
     * Await the completion of this request
     */
    public void await() throws InterruptedException {
        latch.await();
    }

    /**
     * Await the completion of this request (up to the given time interval)
     * @param timeout The maximum time to wait
     * @param unit The unit for the max time
     * @return true if the request completed, false if we timed out
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    /**
     * The base offset for the request (the first offset in the record set)
     */
    public long baseOffset() {
        return baseOffset;
    }

    /**
     * The error thrown (generally on the server) while processing this request
     */
    public RuntimeException error() {
        return error;
    }

    /**
     * The topic and partition to which the record was appended
     */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    /**
     * Has the request completed?
     */
    public boolean completed() {
        return this.latch.getCount() == 0L;
    }
}

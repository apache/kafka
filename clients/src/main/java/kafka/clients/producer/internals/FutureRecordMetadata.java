package kafka.clients.producer.internals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import kafka.clients.producer.RecordMetadata;

/**
 * The future result of a record send
 */
public final class FutureRecordMetadata implements Future<RecordMetadata> {

    private final ProduceRequestResult result;
    private final long relativeOffset;

    public FutureRecordMetadata(ProduceRequestResult result, long relativeOffset) {
        this.result = result;
        this.relativeOffset = relativeOffset;
    }

    @Override
    public boolean cancel(boolean interrupt) {
        return false;
    }

    @Override
    public RecordMetadata get() throws InterruptedException, ExecutionException {
        this.result.await();
        return valueOrError();
    }

    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        boolean occurred = this.result.await(timeout, unit);
        if (!occurred)
            throw new TimeoutException("Timeout after waiting for " + TimeUnit.MILLISECONDS.convert(timeout, unit) + " ms.");
        return valueOrError();
    }

    private RecordMetadata valueOrError() throws ExecutionException {
        if (this.result.error() != null)
            throw new ExecutionException(this.result.error());
        else
            return new RecordMetadata(result.topicPartition(), this.result.baseOffset() + this.relativeOffset);
    }

    public long relativeOffset() {
        return this.relativeOffset;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return this.result.completed();
    }

}

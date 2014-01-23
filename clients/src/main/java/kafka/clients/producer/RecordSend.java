package kafka.clients.producer;

import java.util.concurrent.TimeUnit;

import kafka.clients.producer.internals.ProduceRequestResult;
import kafka.common.errors.ApiException;
import kafka.common.errors.TimeoutException;

/**
 * An asynchronously computed response from sending a record. Calling <code>await()</code> or most of the other accessor
 * methods will block until the response for this record is available. If you wish to avoid blocking provide a
 * {@link kafka.clients.producer.Callback Callback} with the record send.
 */
public final class RecordSend {

    private final long relativeOffset;
    private final ProduceRequestResult result;

    public RecordSend(long relativeOffset, ProduceRequestResult result) {
        this.relativeOffset = relativeOffset;
        this.result = result;
    }

    /**
     * Block until this send has completed successfully. If the request fails, throw the error that occurred in sending
     * the request.
     * @return the same object for chaining of calls
     * @throws TimeoutException if the thread is interrupted while waiting
     * @throws ApiException if the request failed.
     */
    public RecordSend await() {
        result.await();
        if (result.error() != null)
            throw result.error();
        return this;
    }

    /**
     * Block until this send is complete or the given timeout elapses
     * @param timeout the time to wait
     * @param unit the units of the time given
     * @return the same object for chaining
     * @throws TimeoutException if the request isn't satisfied in the time period given or the thread is interrupted
     *         while waiting
     * @throws ApiException if the request failed.
     */
    public RecordSend await(long timeout, TimeUnit unit) {
        boolean success = result.await(timeout, unit);
        if (!success)
            throw new TimeoutException("Request did not complete after " + timeout + " " + unit);
        if (result.error() != null)
            throw result.error();
        return this;
    }

    /**
     * Get the offset for the given message. This method will block until the request is complete and will throw an
     * exception if the request fails.
     * @return The offset
     */
    public long offset() {
        await();
        return this.result.baseOffset() + this.relativeOffset;
    }

    /**
     * Check if the request is complete without blocking
     */
    public boolean completed() {
        return this.result.completed();
    }

    /**
     * Block on request completion and return true if there was an error.
     */
    public boolean hasError() {
        result.await();
        return this.result.error() != null;
    }

    /**
     * Return the error thrown
     */
    public Exception error() {
        result.await();
        return this.result.error();
    }
}

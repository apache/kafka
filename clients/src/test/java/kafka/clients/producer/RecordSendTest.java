package kafka.clients.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import kafka.clients.producer.internals.FutureRecordMetadata;
import kafka.clients.producer.internals.ProduceRequestResult;
import kafka.common.TopicPartition;
import kafka.common.errors.CorruptRecordException;

import org.junit.Test;

public class RecordSendTest {

    private TopicPartition topicPartition = new TopicPartition("test", 0);
    private long baseOffset = 45;
    private long relOffset = 5;

    /**
     * Test that waiting on a request that never completes times out
     */
    @Test
    public void testTimeout() throws Exception {
        ProduceRequestResult request = new ProduceRequestResult();
        FutureRecordMetadata future = new FutureRecordMetadata(request, relOffset);
        assertFalse("Request is not completed", future.isDone());
        try {
            future.get(5, TimeUnit.MILLISECONDS);
            fail("Should have thrown exception.");
        } catch (TimeoutException e) { /* this is good */
        }

        request.done(topicPartition, baseOffset, null);
        assertTrue(future.isDone());
        assertEquals(baseOffset + relOffset, future.get().offset());
    }

    /**
     * Test that an asynchronous request will eventually throw the right exception
     */
    @Test(expected = ExecutionException.class)
    public void testError() throws Exception {
        FutureRecordMetadata future = new FutureRecordMetadata(asyncRequest(baseOffset, new CorruptRecordException(), 50L), relOffset);
        future.get();
    }

    /**
     * Test that an asynchronous request will eventually return the right offset
     */
    @Test
    public void testBlocking() throws Exception {
        FutureRecordMetadata future = new FutureRecordMetadata(asyncRequest(baseOffset, null, 50L), relOffset);
        assertEquals(baseOffset + relOffset, future.get().offset());
    }

    /* create a new request result that will be completed after the given timeout */
    public ProduceRequestResult asyncRequest(final long baseOffset, final RuntimeException error, final long timeout) {
        final ProduceRequestResult request = new ProduceRequestResult();
        new Thread() {
            public void run() {
                try {
                    sleep(timeout);
                    request.done(topicPartition, baseOffset, error);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        return request;
    }

}

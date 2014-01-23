package kafka.clients.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import kafka.clients.producer.internals.ProduceRequestResult;
import kafka.common.TopicPartition;
import kafka.common.errors.CorruptMessageException;
import kafka.common.errors.TimeoutException;

import org.junit.Test;

public class RecordSendTest {

    private TopicPartition topicPartition = new TopicPartition("test", 0);
    private long baseOffset = 45;
    private long relOffset = 5;

    /**
     * Test that waiting on a request that never completes times out
     */
    @Test
    public void testTimeout() {
        ProduceRequestResult request = new ProduceRequestResult();
        RecordSend send = new RecordSend(relOffset, request);
        assertFalse("Request is not completed", send.completed());
        try {
            send.await(5, TimeUnit.MILLISECONDS);
            fail("Should have thrown exception.");
        } catch (TimeoutException e) { /* this is good */
        }

        request.done(topicPartition, baseOffset, null);
        assertTrue(send.completed());
        assertEquals(baseOffset + relOffset, send.offset());
    }

    /**
     * Test that an asynchronous request will eventually throw the right exception
     */
    @Test(expected = CorruptMessageException.class)
    public void testError() {
        RecordSend send = new RecordSend(relOffset, asyncRequest(baseOffset, new CorruptMessageException(), 50L));
        send.await();
    }

    /**
     * Test that an asynchronous request will eventually return the right offset
     */
    @Test
    public void testBlocking() {
        RecordSend send = new RecordSend(relOffset, asyncRequest(baseOffset, null, 50L));
        assertEquals(baseOffset + relOffset, send.offset());
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

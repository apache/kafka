package kafka.clients.producer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import kafka.clients.producer.internals.Metadata;
import kafka.common.Cluster;
import kafka.test.TestUtils;

import org.junit.Test;

public class MetadataTest {

    private long refreshBackoffMs = 100;
    private long metadataExpireMs = 1000;
    private Metadata metadata = new Metadata(refreshBackoffMs, metadataExpireMs);

    @Test
    public void testMetadata() throws Exception {
        long time = 0;
        metadata.update(Cluster.empty(), time);
        assertFalse("No update needed.", metadata.needsUpdate(time));
        metadata.forceUpdate();
        assertFalse("Still no updated needed due to backoff", metadata.needsUpdate(time));
        time += refreshBackoffMs;
        assertTrue("Update needed now that backoff time expired", metadata.needsUpdate(time));
        String topic = "my-topic";
        Thread t1 = asyncFetch(topic);
        Thread t2 = asyncFetch(topic);
        assertTrue("Awaiting update", t1.isAlive());
        assertTrue("Awaiting update", t2.isAlive());
        metadata.update(TestUtils.singletonCluster(topic, 1), time);
        t1.join();
        t2.join();
        assertFalse("No update needed.", metadata.needsUpdate(time));
        time += metadataExpireMs;
        assertTrue("Update needed due to stale metadata.", metadata.needsUpdate(time));
    }

    private Thread asyncFetch(final String topic) {
        Thread thread = new Thread() {
            public void run() {
                metadata.fetch(topic, Integer.MAX_VALUE);
            }
        };
        thread.start();
        return thread;
    }

}

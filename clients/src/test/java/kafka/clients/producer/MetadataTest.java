package kafka.clients.producer;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import kafka.clients.producer.internals.Metadata;
import kafka.common.Cluster;
import kafka.common.Node;
import kafka.common.PartitionInfo;

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
        metadata.update(clusterWith(topic), time);
        t1.join();
        t2.join();
        assertFalse("No update needed.", metadata.needsUpdate(time));
        time += metadataExpireMs;
        assertTrue("Update needed due to stale metadata.", metadata.needsUpdate(time));
    }

    private Cluster clusterWith(String topic) {
        return new Cluster(asList(new Node(0, "localhost", 1969)), asList(new PartitionInfo(topic, 0, 0, new int[0], new int[0])));
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

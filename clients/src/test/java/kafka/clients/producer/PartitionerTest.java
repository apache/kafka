package kafka.clients.producer;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import kafka.clients.producer.internals.Partitioner;
import kafka.common.Cluster;
import kafka.common.Node;
import kafka.common.PartitionInfo;

import org.junit.Test;

public class PartitionerTest {

    private byte[] key = "key".getBytes();
    private byte[] value = "value".getBytes();
    private Partitioner partitioner = new Partitioner();
    private Node node0 = new Node(0, "localhost", 99);
    private Node node1 = new Node(1, "localhost", 100);
    private Node node2 = new Node(2, "localhost", 101);
    private Node[] nodes = new Node[] { node0, node1, node2 };
    private String topic = "test";
    private List<PartitionInfo> partitions = asList(new PartitionInfo(topic, 0, node0, nodes, nodes),
                                                    new PartitionInfo(topic, 1, node1, nodes, nodes),
                                                    new PartitionInfo(topic, 2, null, nodes, nodes));
    private Cluster cluster = new Cluster(asList(node0, node1, node2), partitions);

    @Test
    public void testUserSuppliedPartitioning() {
        assertEquals("If the user supplies a partition we should use it.",
                     0,
                     partitioner.partition(new ProducerRecord("test", 0, key, value), cluster));
    }

    @Test
    public void testKeyPartitionIsStable() {
        int partition = partitioner.partition(new ProducerRecord("test", key, value), cluster);
        assertEquals("Same key should yield same partition",
                     partition,
                     partitioner.partition(new ProducerRecord("test", key, "value2".getBytes()), cluster));
    }

    @Test
    public void testRoundRobinWithDownNode() {
        for (int i = 0; i < partitions.size(); i++) {
            int part = partitioner.partition(new ProducerRecord("test", value), cluster);
            assertTrue("We should never choose a leader-less node in round robin", part >= 0 && part < 2);

        }
    }
}
